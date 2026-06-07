#!/usr/bin/env python3
"""
ebox_mqtt.py — liest alle 3 EBox-Packs via C-Binary, publiziert JSON an MQTT,
schreibt weiterhin in pv_ebox2 DB.

Cron: alle 30s
  * * * * * /usr/bin/python3 /home/pi/python/ebox_mqtt.py >/tmp/ebox_mqtt.log 2>&1
  * * * * * sleep 30 && /usr/bin/python3 /home/pi/python/ebox_mqtt.py >>/tmp/ebox_mqtt.log 2>&1

MQTT topic: ebox/pwr (retained)
JSON: {"soc": 84.3, "power_w": 5400, "current_a": 102.4, "packs": 3, "ts": "2026-06-07T12:00:00"}
"""

import subprocess
import json
import sys
import time
from datetime import datetime

EBOX_BIN   = "/home/pi/sofar/ebox"
DB_SCRIPT  = "/home/pi/python/pv_ebox2.py"
MQTT_HOST  = "192.168.178.218"
MQTT_TOPIC = "ebox/pwr"
EBOX_MULT  = 2.0   # zweite unsichtbare 15kWh EBox

def run_ebox() -> list[str]:
    """Ruft C-Binary auf, gibt alle Ausgabezeilen zurück."""
    try:
        result = subprocess.run(
            [EBOX_BIN, "pwr"],
            capture_output=True, text=True, timeout=15
        )
        return result.stdout.splitlines()
    except Exception as e:
        print(f"ebox binary error: {e}")
        return []

def parse_packs(lines: list[str]) -> list[dict]:
    """Parst Zeilen aus ebox pwr, gibt Liste mit Pack-Dicts zurück."""
    packs = []
    for line in lines:
        line = line.strip()
        if not line or '$' in line or '#' in line or 'Power' in line:
            continue
        parts = line.split()
        if len(parts) < 13:
            continue
        try:
            bat_num = int(parts[0])
            volt_mv = float(parts[1])
            curr_ma = float(parts[2])
            coulomb = float(parts[12].replace('%', ''))
            base_st = parts[8]
            if base_st == 'Absent':
                continue
            packs.append({
                'num':     bat_num,
                'volt_mv': volt_mv,
                'curr_ma': curr_ma,
                'soc':     coulomb,
                'fields':  parts[0:13],
            })
        except (ValueError, IndexError):
            continue
    return packs

def write_db(pack_fields: list[str]):
    """Schreibt einen Pack in pv_ebox2 via pv_ebox2.py."""
    try:
        subprocess.run(
            ['/usr/bin/python3', DB_SCRIPT] + pack_fields,
            timeout=10
        )
    except Exception as e:
        print(f"DB write error: {e}")

def publish_mqtt(payload: dict):
    """Publiziert JSON-Payload als retained Message an ebox/pwr."""
    import paho.mqtt.publish as publish
    publish.single(
        MQTT_TOPIC,
        json.dumps(payload),
        hostname=MQTT_HOST,
        retain=True,
        port=1883,
    )

def main():
    lines = run_ebox()
    if not lines:
        print("Keine Ausgabe vom ebox binary")
        sys.exit(1)

    packs = parse_packs(lines)
    if not packs:
        print("Keine Pack-Daten parsebar")
        sys.exit(1)

    # DB schreiben (alle Packs wie bisher)
    for pack in packs:
        write_db(pack['fields'])

    # Aggregieren: Summe Leistung + Strom, Durchschnitt SOC
    total_w  = sum(p['volt_mv'] * p['curr_ma'] / 1_000_000.0 for p in packs) * EBOX_MULT
    total_a  = sum(p['curr_ma'] for p in packs) / 1000.0 * EBOX_MULT
    avg_soc  = sum(p['soc'] for p in packs) / len(packs)

    payload = {
        'soc':      round(avg_soc, 1),
        'power_w':  round(total_w, 1),
        'current_a': round(total_a, 2),
        'packs':    len(packs),
        'ts':       datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
    }

    print(f"EBox: SOC={avg_soc:.1f}%  P={total_w:.0f}W  I={total_a:.2f}A  ({len(packs)} Packs, ×{EBOX_MULT})")

    try:
        publish_mqtt(payload)
        print(f"MQTT {MQTT_TOPIC}: {json.dumps(payload)}")
    except Exception as e:
        print(f"MQTT publish error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
