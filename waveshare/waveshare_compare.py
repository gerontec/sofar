#!/usr/bin/env python3
"""
waveshare_compare.py — 24h Vergleich: fox2db.py (Pi) vs Waveshare ESP32
Subscriptions: fox2db/state + sofar/state
Meldet nur Abweichungen. Start: jetzt, Ende: 24h später.
"""
import json, time, datetime, signal, sys
from pathlib import Path
import paho.mqtt.client as mqtt

LOG      = '/tmp/waveshare_compare.log'
BROKER   = '192.168.178.218'
DURATION = 24 * 3600

fox_state  = None
fox_trace  = ''
total      = 0
diverge    = 0
start_ts   = time.monotonic()
deadline   = start_ts + DURATION

def log(msg):
    ts   = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG, 'a') as f:
        f.write(line + '\n')

def summary():
    elapsed = int(time.monotonic() - start_ts)
    pct = f"{diverge/total*100:.1f}%" if total else "n/a"
    log(f"SUMMARY: {total} Zyklen, {diverge} Abweichungen ({pct}) nach {elapsed//3600}h{elapsed%3600//60}m")

def on_message(client, userdata, msg):
    global fox_state, fox_trace, total, diverge
    try:
        j = json.loads(msg.payload.decode())
    except Exception:
        return

    if msg.topic == 'fox2db/state':
        fox_state = int(j.get('state', -1))
        fox_trace = j.get('trace', '')

    elif msg.topic == 'sofar/state':
        ws_state = int(j.get('state', -1))
        ws_trace = j.get('trace', '')
        if fox_state is None:
            return
        total += 1
        if ws_state != fox_state:
            diverge += 1
            log(f"DIVERGENZ #{diverge}/{total}: "
                f"fox2db=State{fox_state} ({fox_trace[:50]}) | "
                f"waveshare=State{ws_state} ({ws_trace[:50]})")
        # Stündliche Zusammenfassung
        if total % 60 == 0:
            summary()

def on_signal(sig, frame):
    summary()
    sys.exit(0)

signal.signal(signal.SIGTERM, on_signal)
signal.signal(signal.SIGINT, on_signal)

log(f"Start 24h Vergleich — fox2db/state vs sofar/state (Abweichungen werden gemeldet)")

client = mqtt.Client(client_id="ws_compare_24h", clean_session=True)
client.on_message = on_message
client.connect(BROKER, 1883, 60)
client.subscribe('fox2db/state')
client.subscribe('sofar/state')

while time.monotonic() < deadline:
    client.loop(timeout=1.0)

summary()
client.disconnect()
log("Vergleich beendet.")
