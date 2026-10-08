#!/usr/bin/env python3
"""
sofar_fast.py - fast Sofar reader on the kellertreppe Pi (192.168.178.218): every POLL_SECONDS the few registers
the controllers need, published in the format of pivot2db.py on inverter/power_grid_exchange/json, so that
dbus-pcc-grid / batmonitor (Venus) and the Waveshare ESP see the PCC every 4 s instead of once a minute.
read.py + pivot2db.py (sofar.sh) keep writing the full register set into inverter_data, now every 5 min.

Registers (sofarregister.csv), FC3, slave 1, /dev/ttyUSB32 9600 8N1, read with the C reader mbread:
  0x0488 ActivePower_PCC_Total  I16 x0.01 kW (+ export)
  0x04AF ActivePower_Load_Sys   U16 x0.01 kW
  0x0586 Power_PV1, 0x0589 Power_PV2   U16 x0.01 kW   (block 0x0586:4)
  0x0606 Power_Bat1             I16 x0.01 kW (+ charge), 0x0608 SOC_Bat1 U16 %   (block 0x0606:3)

Bus sharing: each read holds /tmp/sofar.lock (the lock of sofar.sh) for its ~150 ms; while read.py runs the
lock is busy and the cycle is skipped. sofar.sh waits for the lock (flock -w) instead of skipping its run.
Test 2026-10-08: 1 read/s of 2 registers for 5 min, all answers OK, median 91 ms, read.py unaffected.

Long-term stability (runs as systemd service sofar_fast.service):
- every read is a fresh, short mbread process: the serial port is opened and closed per read, a USB reset or a
  hung read can never leave stale state behind (subprocess timeout 5 s)
- paho reconnects by itself; the outgoing queue is capped so a broker outage cannot grow memory
- every exception in the loop is logged and the loop goes on; systemd restarts on exit (Restart=always),
  MemoryMax caps memory
- watchdog (WatchdogSec=60 in the unit) bound to real data (2026-10-08): WATCHDOG=1 only while the last verified
  value is younger than DATA_MAX_AGE. Verified = (1) the PCC was really read (not substituted) and PCC, Bat1, SOC
  and PV are within plausible ranges, and (2) end to end: sofar_fast subscribes its own topic, and only a message
  that came back through the broker with source sofar_fast and a timestamp at most LOOP_MAX_DELAY old counts.
  Dead Sofar, stuck adapter, garbage values or a broken broker path -> no ping -> systemd restarts the service
  after 60 s (in the journal as a watchdog timeout).

Payload as pivot2db.py (but not retained, QoS 0: a value every 4 s; pivot2db keeps its retained one every 5 min): the bare 'ActivePower_PCC_Total' key first (the ESP parser finds keys with strstr and
'..._avg4' / '..._avg5' share the prefix); avg5 = mean over 5 min, avg4 = mean of the last 4 valid PCC values,
used as substitute (PCC_substituted 1) when the PCC read fails.
"""
import fcntl
import json
import logging
import os
import socket
import subprocess
import time
from collections import deque
from datetime import datetime

import paho.mqtt.client as mqtt

POLL_SECONDS = 4
MBREAD = ['/home/pi/python/mbread', '-p', '/dev/ttyUSB32', '-b', '9600', '-s', '1', '-f', '3', '-t', '300', '-r', '2']
BLOCKS = ['0x0488:1', '0x04AF:1', '0x0586:4', '0x0606:3']
LOCK_FILE = '/tmp/sofar.lock'
MQTT_HOST, MQTT_TOPIC = '127.0.0.1', 'inverter/power_grid_exchange/json'
AVG5_SECONDS = 300
DATA_MAX_AGE = 45          # s without a verified value -> no WATCHDOG=1 (WatchdogSec=60 restarts)
LOOP_MAX_DELAY = 10        # s: own message must come back through the broker within this time of its timestamp
LIMITS = {'ActivePower_PCC_Total': (-30, 30), 'Power_Bat1': (-10, 10), 'SOC_Bat1': (0, 100),
          'Power_PV1': (0, 30), 'Power_PV2': (0, 30)}   # kW / %: plausible ranges

log = logging.getLogger('sofar_fast')


def sd_notify(msg):
    """systemd notify without the python3-systemd package (READY=1 / WATCHDOG=1)"""
    addr = os.environ.get('NOTIFY_SOCKET')
    if not addr:
        return
    if addr.startswith('@'):
        addr = '\0' + addr[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
            s.sendto(msg.encode(), addr)
    except OSError:
        pass


def s16(v):
    return v - 65536 if v >= 32768 else v


def read_registers():
    """one mbread call under the sofar.sh lock; returns {address: value} or None if the bus is busy"""
    with open(LOCK_FILE, 'a') as lf:
        try:
            fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return None                      # read.py / sofar.sh is on the bus
        try:
            out = subprocess.run(MBREAD + BLOCKS, capture_output=True, text=True, timeout=5).stdout
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)
    regs = {}
    for line in out.splitlines():            # "OK <base> <count> v1 v2 ..." / "ERR <base> <count> <reason>"
        p = line.split()
        if len(p) >= 3 and p[0] == 'OK':
            base = int(p[1])
            for i, v in enumerate(p[3:]):
                regs[base + i] = int(v)
        elif p and p[0] == 'ERR':
            log.warning('read failed: %s', line)
    return regs


def plausible(payload):
    """the PCC was really read and every checked value is present and within LIMITS"""
    if payload['PCC_substituted']:
        return False
    for k, (lo, hi) in LIMITS.items():
        v = payload.get(k)
        if v is None or not lo <= v <= hi:
            log.warning('implausible %s=%s', k, v)
            return False
    return True


def on_message(client, state, msg):
    """loopback check: our own message came back through the broker, with a fresh timestamp"""
    try:
        d = json.loads(msg.payload)
        if d.get('source') != 'sofar_fast' or not d.get('plausible'):
            return
        age = (datetime.now() - datetime.fromisoformat(d['timestamp'])).total_seconds()
        if 0 <= age <= LOOP_MAX_DELAY:
            state['verified'] = time.monotonic()
    except (ValueError, KeyError, TypeError):
        pass


def on_connect(client, state, flags, reason_code, properties):
    client.subscribe(MQTT_TOPIC, qos=0)


def cycle(client, state):
    regs = read_registers()
    if not regs:
        return
    now = time.time()
    hist_pcc, hist_bat, last4 = state['hist_pcc'], state['hist_bat'], state['last4']
    pcc = round(s16(regs[0x0488]) * 0.01, 2) if 0x0488 in regs else None
    bat1 = round(s16(regs[0x0606]) * 0.01, 2) if 0x0606 in regs else None
    soc = regs.get(0x0608)
    if soc:
        state['last_soc'] = float(soc)
    for h, v in ((hist_pcc, pcc), (hist_bat, bat1)):
        if v is not None:
            h.append((now, v))
        while h and now - h[0][0] > AVG5_SECONDS:
            h.popleft()
    if pcc is not None:
        last4.append(pcc)
    avg4 = round(sum(last4) / len(last4), 2) if last4 else None
    payload = {
        'ActivePower_PCC_Total': pcc if pcc is not None else avg4,
        'ActivePower_PCC_Total_avg5': round(sum(v for _, v in hist_pcc) / len(hist_pcc), 2) if hist_pcc else None,
        'ActivePower_PCC_Total_avg4': avg4,
        'ActivePower_PCC_Total_raw': pcc,
        'PCC_substituted': 0 if pcc is not None else 1,
        'Power_Bat1': bat1,
        'Power_Bat1_avg5': round(sum(v for _, v in hist_bat) / len(hist_bat), 2) if hist_bat else None,
        'SOC_Bat1': state['last_soc'],
        'Power_PV1': round(regs[0x0586] * 0.01, 2) if 0x0586 in regs else None,
        'Power_PV2': round(regs[0x0589] * 0.01, 2) if 0x0589 in regs else None,
        'ActivePower_Load_Sys': round(regs[0x04AF] * 0.01, 2) if 0x04AF in regs else None,
        'source': 'sofar_fast',
        'timestamp': datetime.now().isoformat(),
    }
    payload['plausible'] = 1 if plausible(payload) else 0
    if payload['ActivePower_PCC_Total'] is not None:
        client.publish(MQTT_TOPIC, json.dumps(payload), qos=0, retain=False)   # 4 s stream: no retain


def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    state = {'hist_pcc': deque(), 'hist_bat': deque(), 'last4': deque(maxlen=4), 'last_soc': None,
             'verified': time.monotonic()}         # start grace: DATA_MAX_AGE to the first verified value
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id='sofar_fast', userdata=state)
    client.on_connect = on_connect
    client.on_message = on_message
    client.max_queued_messages_set(20)
    client.reconnect_delay_set(1, 30)
    client.connect_async(MQTT_HOST, 1883, 60)
    client.loop_start()
    sd_notify('READY=1')
    warned = False
    while True:
        t0 = time.monotonic()
        try:
            cycle(client, state)
        except Exception:
            log.exception('cycle failed')
        age = time.monotonic() - state['verified']
        if age < DATA_MAX_AGE:
            sd_notify('WATCHDOG=1')
            if warned:
                log.info('verified data again')
            warned = False
        elif not warned:
            log.error('no verified data for %.0f s: watchdog not fed, systemd restarts in <= 60 s', age)
            warned = True
        time.sleep(max(0.0, POLL_SECONDS - (time.monotonic() - t0)))


if __name__ == '__main__':
    main()
