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

Every frame is logged as one JSON line in ~/python/v5capture/frames-YYYYMMDD.jsonl (field "action": pass/drop).
The logger itself polls the inverter (Data collection mode); read.py / sofar_fast.py read through the logger's
Modbus TCP gateway on port 8899 (TCP_FRAMER 'socket').
"""
import asyncio
import json
import logging
import os
from datetime import datetime

LISTEN_HOST, LISTEN_PORT = '0.0.0.0', 10000
CLOUD_HOST, CLOUD_PORT = 'data1.solarmanpv.com', 10000
LOG_DIR = os.path.expanduser('~/python/v5capture')
DOWN_ACKS = {0x1110, 0x1210, 0x1310, 0x1710, 0x1810}
CTRL_MODBUS_REQUEST = 0x4510
READ_FUNCTION_CODES = {1, 2, 3, 4}
MODBUS_OFFSET = 11 + 15          # V5 header (11) + request payload header (15) -> Modbus RTU frame (slave, fc, ...)

log = logging.getLogger('v5_relay')


def ctrl_of(frame):
    if len(frame) >= 11 and frame[0] == 0xA5:
        return int.from_bytes(frame[3:5], 'little')
    return None


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


def record(direction, frame, action, reason=''):
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
    if action == 'drop':
        log.warning('DROPPED %s from cloud: %s', reason, frame.hex())


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


async def pump(reader, writer, direction):
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
                record(direction, fr, 'pass' if ok else 'drop', reason)
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
    tasks = [asyncio.create_task(pump(lreader, cwriter, 'up')), asyncio.create_task(pump(creader, lwriter, 'down'))]
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
    asyncio.run(main())
