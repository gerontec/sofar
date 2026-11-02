#!/usr/bin/env python3
"""v5_relay.py - Solarman V5 relay between the Sofar LSE-3 logger (192.168.178.108) and the Solarman cloud that
lets the cloud read but never write (2026-11-02).

logger (Server A = 192.168.178.218:10000) -> this relay -> data1.solarmanpv.com:10000

up   (logger -> cloud): passed unchanged (hello 0x4110, data 0x4210, 0x4310, heartbeat 0x4710, 0x4810, ...)
down (cloud -> logger): only these are passed
     - acknowledgements 0x1110 0x1210 0x1310 0x1710 0x1810
     - 0x4510 Modbus pass-through requests with a READ function code (1, 2, 3, 4)
     everything else is dropped and logged: 0x4510 with write codes (5, 6, 15, 16, 22, 23, ...), and every other
     command (logger configuration, firmware update, unknown).

Forensics: every dropped frame is decoded (slave, function code, register + name from sofarregister.csv, written
values) and written to ~/python/v5capture/forensics-YYYYMMDD.jsonl together with the session (logger and cloud
IP) and the FORENSIC_BEFORE frames before it; every frame of the next FORENSIC_AFTER_S seconds follows as "after".
Alert mail to gh@heissa.de ALERT_COLLECT_S after the first drop with the whole decoded sequence, at most one per
hour (drops in between are collected and sent when the hour is over).
Every frame is logged as one JSON line in ~/python/v5capture/frames-YYYYMMDD.jsonl (field "action": pass/drop).
The logger itself polls the inverter (Data collection mode); read.py / sofar_fast.py read through the logger's
Modbus TCP gateway on port 8899 (TCP_FRAMER 'socket').
"""
import asyncio
import collections
import json
import logging
import os
import smtplib
import sys
import threading
import time
from datetime import datetime
from email.mime.text import MIMEText

LISTEN_HOST, LISTEN_PORT = '0.0.0.0', 10000
CLOUD_HOST, CLOUD_PORT = 'data1.solarmanpv.com', 10000
LOG_DIR = os.path.expanduser('~/python/v5capture')
DOWN_ACKS = {0x1110, 0x1210, 0x1310, 0x1710, 0x1810}
CTRL_MODBUS_REQUEST = 0x4510
READ_FUNCTION_CODES = {1, 2, 3, 4}
MODBUS_OFFSET = 11 + 15          # V5 header (11) + request payload header (15) -> Modbus RTU frame (slave, fc, ...)

# alert mail on every dropped cloud frame (as r290mb.py: smtplib to heissa over the VPN), at most one per hour;
# drops during the quiet hour are counted and reported with the next mail
ALERT_MAIL_TO = 'gh@heissa.de'
ALERT_MAIL_FROM = 'v5relay@heissa.de'
ALERT_SMTP_HOST, ALERT_SMTP_PORT = '10.8.0.1', 25
ALERT_MIN_INTERVAL = 3600        # s between two alert mails
ALERT_COLLECT_S = 120            # s after the first drop before the mail goes out (whole sequence in one mail)

log = logging.getLogger('v5_relay')
alert_state = {'last_mail': 0.0, 'pending': [], 'lock': threading.Lock()}


def send_mail(subject, body):
    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = ALERT_MAIL_FROM
    msg['To'] = ALERT_MAIL_TO
    with smtplib.SMTP(ALERT_SMTP_HOST, ALERT_SMTP_PORT, timeout=15) as s:
        s.sendmail(ALERT_MAIL_FROM, [ALERT_MAIL_TO], msg.as_string())


def alert_drop(reason, frame, text='', session_info=None):
    """collect a dropped frame; the mail goes out ALERT_COLLECT_S after the first drop (so a whole write sequence
    is in one mail), but never sooner than ALERT_MIN_INTERVAL after the previous mail"""
    with alert_state['lock']:
        alert_state['pending'].append({'t': datetime.now().isoformat(timespec='milliseconds'), 'reason': reason,
                                       'text': text, 'hex': frame.hex(), 'session': session_info})
        if alert_state.get('timer'):
            return
        delay = max(ALERT_COLLECT_S, alert_state['last_mail'] + ALERT_MIN_INTERVAL - time.time())
        alert_state['timer'] = threading.Timer(delay, _mail_drops)
        alert_state['timer'].daemon = True
        alert_state['timer'].start()


def _mail_drops():
    with alert_state['lock']:
        items, alert_state['pending'] = alert_state['pending'], []
        alert_state['timer'] = None
        alert_state['last_mail'] = time.time()
    if not items:
        return
    lines = []
    for it in items[:100]:
        lines.append('%s  [%s]  %s' % (it['t'], it['reason'], it['text']))
        lines.append('    V5 frame: %s' % it['hex'])
    if len(items) > 100:
        lines.append('... and %d more (see forensic log)' % (len(items) - 100))
    sessions = sorted({json.dumps(it['session'], sort_keys=True) for it in items if it['session']})
    body = ('The Solarman cloud sent %d command(s) to the Sofar logger 192.168.178.108.\n'
            'v5_relay.py on 192.168.178.218 dropped them: NOTHING was passed to the logger or the inverter.\n\n'
            'Sequence (time, reason, decoded request, raw frame):\n%s\n\n'
            'Session(s): %s\n\n'
            'Forensic record (decoded request, %d frames before, all frames %d s after, cloud and logger IP):\n'
            '  ~/python/v5capture/forensics-%s.jsonl on 192.168.178.218\n'
            '  journalctl -u v5_relay | grep DROPPED\n'
            'Next mail at the earliest in %d min; drops until then are collected.\n'
            % (len(items), '\n'.join(lines), '; '.join(sessions) or '-', FORENSIC_BEFORE, FORENSIC_AFTER_S,
               datetime.now().strftime('%Y%m%d'), ALERT_MIN_INTERVAL // 60))
    try:
        send_mail('Solarman cloud write attempt blocked (%d): %s' % (len(items), items[0]['text'] or items[0]['reason']),
                  body)
        log.info('alert mail sent (%d drops)', len(items))
    except Exception as e:
        log.error('alert mail failed: %s', e)
        with alert_state['lock']:           # keep them, try again in ALERT_COLLECT_S
            alert_state['pending'] = items + alert_state['pending']
            alert_state['last_mail'] = 0.0
            if not alert_state.get('timer'):
                alert_state['timer'] = threading.Timer(ALERT_COLLECT_S, _mail_drops)
                alert_state['timer'].daemon = True
                alert_state['timer'].start()


def ctrl_of(frame):
    if len(frame) >= 11 and frame[0] == 0xA5:
        return int.from_bytes(frame[3:5], 'little')
    return None


# ---- forensics: decode dropped frames, keep the session context around them ----
REGISTER_CSV = os.path.expanduser('~/python/sofarregister.csv')
FORENSIC_BEFORE = 30             # frames (both directions) kept before a drop
FORENSIC_AFTER_S = 120           # s after a drop during which every frame of the session goes to the forensic log
_register_names = None


def register_name(addr):
    """field name of a Sofar register from sofarregister.csv ("name";"hex addr or range";"field";...)"""
    global _register_names
    if _register_names is None:
        _register_names = {}
        try:
            import csv
            with open(REGISTER_CSV, encoding='utf-8') as f:
                for row in csv.reader(f, delimiter=';'):
                    if len(row) < 3 or not row[1].strip():
                        continue
                    a = row[1].replace(' ', '').replace('____', '-')
                    try:
                        lo, _, hi = a.partition('-')
                        lo = int(lo, 16)
                        hi = int(hi, 16) if hi else lo
                    except ValueError:
                        continue
                    for x in range(lo, min(hi, lo + 64) + 1):
                        _register_names.setdefault(x, row[2].strip() or row[0].strip())
        except OSError as e:
            log.warning('register names not loaded: %s', e)
    return _register_names.get(addr, '?')


def decode_frame(frame):
    """V5 header + (for 0x4510) the Modbus request: what the cloud wanted to do"""
    d = {'ctrl': '0x%04X' % ctrl_of(frame)} if ctrl_of(frame) is not None else {'raw': True}
    if 'raw' in d:
        return d
    d['seq'] = frame[5:7].hex()
    d['logger_sn'] = int.from_bytes(frame[7:11], 'little')
    if ctrl_of(frame) != CTRL_MODBUS_REQUEST or len(frame) < MODBUS_OFFSET + 4:
        d['payload'] = frame[11:-2].hex()
        return d
    m = frame[MODBUS_OFFSET:-2]                     # Modbus RTU frame incl. its CRC
    slave, fc = m[0], m[1]
    d.update({'modbus': m.hex(), 'slave': slave, 'fc': fc})
    u16 = lambda i: int.from_bytes(m[i:i + 2], 'big')
    try:
        if fc in (5, 6):
            d['writes'] = [(u16(2), u16(4))]
        elif fc == 16:
            qty, n = u16(4), m[6]
            d['writes'] = [(u16(2) + i, u16(7 + 2 * i)) for i in range(min(qty, n // 2))]
        elif fc == 15:
            d['coils'] = {'start': u16(2), 'qty': u16(4), 'bits': m[7:7 + m[6]].hex()}
        elif fc == 23:
            qty, n = u16(8), m[10]
            d['reads'] = {'start': u16(2), 'qty': u16(4)}
            d['writes'] = [(u16(6) + i, u16(11 + 2 * i)) for i in range(min(qty, n // 2))]
        elif fc in READ_FUNCTION_CODES:
            d['reads'] = {'start': u16(2), 'qty': u16(4)}
    except IndexError:
        d['truncated'] = True
    if 'writes' in d:
        d['writes'] = [{'reg': '0x%04X' % a, 'name': register_name(a), 'value': v, 'hex': '0x%04X' % v}
                       for a, v in d['writes']]
    return d


def describe(dec):
    """one readable line per dropped frame for the mail"""
    if 'writes' in dec:
        return 'FC %d slave %d: ' % (dec['fc'], dec['slave']) + ', '.join(
            '%s %s = %d (%s)' % (w['reg'], w['name'], w['value'], w['hex']) for w in dec['writes'])
    if 'coils' in dec:
        return 'FC 15 slave %d: coils %s' % (dec['slave'], dec['coils'])
    if 'fc' in dec:
        return 'FC %d slave %d: modbus %s' % (dec['fc'], dec['slave'], dec['modbus'])
    return 'ctrl %s payload %s' % (dec.get('ctrl', 'raw'), dec.get('payload', ''))


def forensic_write(rec):
    path = os.path.join(LOG_DIR, 'forensics-%s.jsonl' % datetime.now().strftime('%Y%m%d'))
    with open(path, 'a') as f:
        f.write(json.dumps(rec) + '\n')


def down_allowed(frame):
    """(allowed, reason) for a frame from the cloud"""
    ctrl = ctrl_of(frame)
    if ctrl in DOWN_ACKS:
        return True, 'ack'
    if ctrl == CTRL_MODBUS_REQUEST:
        if len(frame) > MODBUS_OFFSET + 1:
            fc = frame[MODBUS_OFFSET + 1]
            if fc in READ_FUNCTION_CODES:
                return True, 'modbus read fc %d' % fc
            return False, 'modbus WRITE fc %d' % fc
        return False, 'modbus request too short'
    if ctrl is None:
        return False, 'not a V5 frame'
    return False, 'command 0x%04X' % ctrl


def record(direction, frame, action, reason='', session=None):
    rec = {'t': datetime.now().isoformat(timespec='milliseconds'), 'dir': direction, 'len': len(frame),
           'action': action, 'hex': frame.hex()}
    ctrl = ctrl_of(frame)
    if ctrl is not None:
        rec['ctrl'] = '0x%04X' % ctrl
        rec['seq'] = frame[5:7].hex()
    if reason:
        rec['reason'] = reason
    path = os.path.join(LOG_DIR, 'frames-%s.jsonl' % datetime.now().strftime('%Y%m%d'))
    with open(path, 'a') as f:
        f.write(json.dumps(rec) + '\n')
    if session is None:
        return
    if action == 'drop':
        dec = decode_frame(frame)
        text = describe(dec)
        log.warning('DROPPED %s from cloud: %s | %s', reason, text, frame.hex())
        forensic_write({'event': 'drop', 't': rec['t'], 'reason': reason, 'decoded': dec, 'hex': frame.hex(),
                        'session': session['info'], 'context_before': list(session['history'])})
        session['after_until'] = time.monotonic() + FORENSIC_AFTER_S
        alert_drop(reason, frame, text, session['info'])
    elif time.monotonic() < session['after_until']:
        forensic_write({'event': 'after', 'session_id': session['info']['id'], **rec})
    session['history'].append(rec)


def split_frames(buf):
    """complete V5 frames from buf: A5 | len u16 LE | ctrl u16 | seq u16 | sn u32 | payload[len] | checksum | 15"""
    frames = []
    while buf:
        if buf[0] != 0xA5:
            i = buf.find(b'\xa5')
            n = i if i > 0 else len(buf)
            frames.append(bytes(buf[:n]))
            del buf[:n]
            continue
        if len(buf) < 3:
            break
        total = int.from_bytes(buf[1:3], 'little') + 13
        if len(buf) < total:
            break
        frames.append(bytes(buf[:total]))
        del buf[:total]
    return frames


async def pump(reader, writer, direction, session):
    """forward whole frames; the down direction is filtered"""
    buf = bytearray()
    try:
        while True:
            data = await reader.read(4096)
            if not data:
                break
            buf += data
            for fr in split_frames(buf):
                if direction == 'down':
                    ok, reason = down_allowed(fr)
                else:
                    ok, reason = True, ''
                record(direction, fr, 'pass' if ok else 'drop', reason, session)
                if ok:
                    writer.write(fr)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        writer.close()


async def handle(lreader, lwriter):
    peer = lwriter.get_extra_info('peername')
    log.info('logger connected %s', peer)
    try:
        creader, cwriter = await asyncio.wait_for(asyncio.open_connection(CLOUD_HOST, CLOUD_PORT), 10)
    except Exception as e:
        log.error('cloud connect failed: %s', e)
        lwriter.close()
        return
    cpeer = cwriter.get_extra_info('peername')
    session = {'info': {'id': datetime.now().strftime('%Y%m%dT%H%M%S'), 'logger': '%s:%s' % peer[:2],
                        'cloud': '%s:%s' % cpeer[:2] if cpeer else CLOUD_HOST,
                        'started': datetime.now().isoformat(timespec='seconds')},
               'history': collections.deque(maxlen=FORENSIC_BEFORE), 'after_until': 0.0}
    log.info('session %s: logger %s <-> cloud %s', session['info']['id'], session['info']['logger'],
             session['info']['cloud'])
    tasks = [asyncio.create_task(pump(lreader, cwriter, 'up', session)),
             asyncio.create_task(pump(creader, lwriter, 'down', session))]
    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for t in tasks:
        t.cancel()
    cwriter.close()
    lwriter.close()
    log.info('session closed %s', peer)


async def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    os.makedirs(LOG_DIR, exist_ok=True)
    server = await asyncio.start_server(handle, LISTEN_HOST, LISTEN_PORT)
    log.info('relay listening on %s -> %s:%s (cloud may read, never write)', LISTEN_PORT, CLOUD_HOST, CLOUD_PORT)
    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    if '--test-mail' in sys.argv[1:]:          # send one [TEST] alert mail and exit
        send_mail('[TEST] Solarman cloud write attempt blocked',
                  'Test of the v5_relay.py alert mail from 192.168.178.218, nothing was dropped.\n')
        print('test mail sent to', ALERT_MAIL_TO)
        sys.exit(0)
    asyncio.run(main())
