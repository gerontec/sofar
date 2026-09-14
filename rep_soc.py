#!/usr/bin/env python3
"""rep_soc.py – Tagesprognose des EBox-SOC (SOC2) vom Waveshare-ESP.

Liest eine Meldung von sofar/state (ESP sendet ~1x/min) und zeigt:
  exp_max   / exp_max_h    neue Prognose: Messung + Wetter-Tagesprognose (fw >= 3.14.0)
  exp_max_p / exp_max_p_h  alte Prognose: nur gemessene Bewoelkung fortgeschrieben
  eta_k / need_kwh         kWh je SOC-% und Restbedarf bis 100 % (fw >= 3.14.1)

Unten folgt immer die komplette Rohmeldung als JSON.

Aufruf: ./rep_soc.py [--timeout SEKUNDEN]
"""
import argparse
import json
import sys
import time
from datetime import datetime

import paho.mqtt.client as mqtt

BROKER = '192.168.178.218'
TOPIC = 'sofar/state'


def fetch(timeout: float):
    box = {}

    def on_message(_c, _u, msg):
        try:
            box['d'] = json.loads(msg.payload.decode())
        except (ValueError, UnicodeDecodeError):
            pass

    c = mqtt.Client()
    c.on_message = on_message
    c.connect(BROKER, 1883, 30)
    c.subscribe(TOPIC)
    c.loop_start()
    t0 = time.time()
    while 'd' not in box and time.time() - t0 < timeout:
        time.sleep(0.2)
    c.loop_stop()
    c.disconnect()
    return box.get('d')


def uhr(h):
    """Lokale Dezimalstunde -> HH:MM, '-' wenn nicht gesetzt."""
    if h is None or h <= 0:
        return '-'
    m = int(round(h * 60))
    return f'{m // 60:02d}:{m % 60:02d}'


def prozent(v):
    return '-' if v is None or v < 0 else f'{v:.1f} %'


def main():
    ap = argparse.ArgumentParser(description='Prognose max. EBox-SOC heute')
    ap.add_argument('--timeout', type=float, default=75, help='Wartezeit auf sofar/state [s]')
    args = ap.parse_args()

    d = fetch(args.timeout)
    if d is None:
        print(f'Keine Meldung auf {TOPIC} innerhalb {args.timeout:.0f} s', file=sys.stderr)
        return 1

    print(f"SOC-Prognose EBox  |  {datetime.now():%d.%m.%Y %H:%M:%S}")
    print('-' * 56)
    print(f"{'':30} {'max. SOC2':>10}  {'Uhrzeit':>7}")
    print(f"{'Neu (Messung + Wetter)':30} {prozent(d.get('exp_max')):>10}  {uhr(d.get('exp_max_h')):>7}")
    if 'exp_max_p' in d:
        print(f"{'Alt (nur Messung)':30} {prozent(d.get('exp_max_p')):>10}  {uhr(d.get('exp_max_p_h')):>7}")
    print('-' * 56)
    print(f"SOC2 (EBox)   {d.get('soc2', '-'):>6} %    EBox   {d.get('ebox', '-'):>6} W   Stufe {d.get('state', '-')}")
    print(f"SOC1 (Sofar)  {d.get('soc1', '-'):>6} %    Akku   {d.get('bat1', '-'):>6} W   PCC {d.get('pcc', '-')} W")
    ratio = d.get('ratio_ist')
    wx = d.get('wx_kt')
    print(f"Bewoelkung jetzt: {'-' if ratio is None or ratio < 0 else f'{ratio * 100:.0f} % unter Klarhimmel'}"
          f"   |   Wetter-kt heute: {'-' if wx is None or wx < 0 else f'{wx:.2f}'}")
    if 'need_kwh' in d:
        k, need = d.get('eta_k'), d.get('need_kwh')
        quelle = '' if k is None or k < 0 else (' (Standard)' if abs(k - 0.32) < 0.0005 else ' (gemessen)')
        print(f"Restbedarf bis 100 %: {'-' if need is None or need < 0 else f'{need:.1f} kWh'}"
              f"   k = {'-' if k is None or k < 0 else f'{k:.3f} kWh/%'}{quelle}")
    print(f"Trace: {d.get('trace', '')}")
    print('-' * 56)
    print(f'{TOPIC} (komplett):')
    print(json.dumps(d, ensure_ascii=False, indent=1))
    return 0


if __name__ == '__main__':
    sys.exit(main())
