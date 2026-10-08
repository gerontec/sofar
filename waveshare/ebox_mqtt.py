#!/usr/bin/env python3
"""
ebox_mqtt.py — liest alle 3 EBox-Packs via C-Binary, publiziert JSON an MQTT,
schreibt weiterhin in pv_ebox2 DB.

Cron: alle 30s (MQTT), DB-Write intern auf 1×/min gedrosselt
  * * * * * /usr/bin/python3 /home/pi/python/ebox_mqtt.py >/tmp/ebox_mqtt.log 2>&1
  * * * * * sleep 30 && /usr/bin/python3 /home/pi/python/ebox_mqtt.py >>/tmp/ebox_mqtt.log 2>&1

MQTT topic: ebox/pwr (retained)
JSON: {"soc": 84.3, "power_w": 5400, "current_a": 102.4, "voltage_v": 53.1, "voltage_mv": 53100, "vhigh_mv": 3420, "vlow_mv": 3301,
       "volt_st": "Normal", "packs": 3, "ts": "2026-06-07T12:00:00"}
Also published to the Venus broker on this host (VENUS_MQTT_HOST), read there by dbus-ebox-battery.
Factor ("mult"): x1 = EBox values (normal case). Fallbacks for the two 15 kWh stacks (approximation):
  CAN BMS of Stack1 silent  -> EBox power/current x2 ("source": "ebox", "mult": 2)
  EBox reader fails         -> CAN BMS (0x355/0x356/0x373) power/current x2 ("source": "can_bms", "mult": 2)
"""

import subprocess
import json
import sys
import time
from datetime import datetime

EBOX_BIN        = "/home/pi/sofar/ebox"
DB_SCRIPT       = "/home/pi/python/pv_ebox2.py"
MQTT_HOST       = "192.168.178.218"
MQTT_TOPIC      = "ebox/pwr"
VENUS_MQTT_HOST = "127.0.0.1"   # Venus OS subsystem (nspawn, host network)
# Power/current factor: x1 = this EBox stack only (normal case). x2 only as fallback when the CAN BMS of the
# other 15 kWh stack (Stack1 MUST on can0) is silent: then the EBox stands in for both stacks (approximation).
CAN_IF          = "can0"
CAN_SOC_FILTER  = "355:7FF"      # SoC/SoH frame of the CAN BMS, sent about every second
CAN_TIMEOUT_MS  = 3000

def can_bms_alive() -> bool:
    """True if the CAN BMS sent its SoC frame within CAN_TIMEOUT_MS."""
    try:
        r = subprocess.run(["candump", "-n", "1", "-T", str(CAN_TIMEOUT_MS), f"{CAN_IF},{CAN_SOC_FILTER}"],
                           capture_output=True, text=True, timeout=CAN_TIMEOUT_MS / 1000 + 2)
        return bool(r.stdout.strip())
    except Exception as e:
        print(f"candump error: {e}")
        return False

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
                'vlow_mv':  float(parts[6]),
                'vhigh_mv': float(parts[7]),
                'volt_st':  parts[9],
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

def can_bms_payload() -> dict | None:
    """Fallback when the EBox reader fails: the CAN BMS of the other stack (Stack1 MUST) x2 stands in for both.
    Frames (Pylontech/Victron CAN-BMS protocol): 0x355 SoC, 0x356 U [0.01 V] / I [0.1 A, signed], 0x373 cell min/max [mV]."""
    try:
        r = subprocess.run(["timeout", "2.5", "candump", f"{CAN_IF},355:7FF,356:7FF,373:7FF"],
                           capture_output=True, text=True, timeout=5)
    except Exception as e:
        print(f"candump error: {e}")
        return None
    f = {}
    for line in r.stdout.splitlines():
        parts = line.split()           # can0  356   [8]  83 14 A3 FF DE 00 00 00
        if len(parts) >= 4 and parts[0] == CAN_IF:
            f[parts[1].upper()] = bytes(int(b, 16) for b in parts[3:])
    if '355' not in f or '356' not in f:
        return None
    soc = int.from_bytes(f['355'][0:2], 'little')
    volt = int.from_bytes(f['356'][0:2], 'little') / 100.0
    curr = int.from_bytes(f['356'][2:4], 'little', signed=True) / 10.0
    cells = f.get('373')
    return {
        'soc': float(soc),
        'mult': 2.0,
        'source': 'can_bms',
        'power_w': round(volt * curr * 2.0, 1),
        'current_a': round(curr * 2.0, 2),
        'voltage_v': round(volt, 2),
        'voltage_mv': round(volt * 1000),
        'vhigh_mv': int.from_bytes(cells[2:4], 'little') if cells else None,
        'vlow_mv': int.from_bytes(cells[0:2], 'little') if cells else None,
        'volt_st': 'Normal',
        'packs': 0,
        'ts': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
    }

def publish_fallback() -> None:
    """EBox reader failed: publish the CAN BMS values x2, or exit 1 if the CAN BMS is silent too."""
    payload = can_bms_payload()
    if payload is None:
        print("CAN BMS silent too, nothing published")
        sys.exit(1)
    print(f"Fallback CAN BMS x2: SOC={payload['soc']:.0f}%  P={payload['power_w']:.0f}W  I={payload['current_a']:.2f}A")
    for host in (VENUS_MQTT_HOST, MQTT_HOST):
        try:
            publish_mqtt(payload, host)
            print(f"MQTT {host} ebox/pwr: ok")
        except Exception as e:
            print(f"MQTT {host} error: {e}")
    sys.exit(0)

def publish_mqtt(payload: dict, host: str = MQTT_HOST):
    """Publiziert JSON-Payload als retained Message an ebox/pwr."""
    import paho.mqtt.publish as publish
    publish.single(
        MQTT_TOPIC,
        json.dumps(payload),
        hostname=host,
        retain=True,
        port=1883,
    )

def main():
    lines = run_ebox()
    if not lines:
        print("Keine Ausgabe vom ebox binary")
        publish_fallback()

    packs = parse_packs(lines)
    if not packs:
        print("Keine Pack-Daten parsebar")
        publish_fallback()

    # DB-Write nur beim ersten Halbminuten-Lauf (:00s)
    if datetime.now().second < 30:
        for pack in packs:
            write_db(pack['fields'])
        print("DB: geschrieben")
    else:
        print("DB: übersprungen (30s-Lauf)")

    # Aggregieren: Summe Leistung + Strom, Durchschnitt SOC
    ebox_mult = 1.0 if can_bms_alive() else 2.0
    total_w  = sum(p['volt_mv'] * p['curr_ma'] / 1_000_000.0 for p in packs) * ebox_mult
    total_a  = sum(p['curr_ma'] for p in packs) / 1000.0 * ebox_mult
    avg_soc  = sum(p['soc'] for p in packs) / len(packs)

    payload = {
        'soc':      round(avg_soc, 1),
        'mult':     ebox_mult,
        'source':   'ebox',
        'power_w':  round(total_w, 1),
        'current_a': round(total_a, 2),
        'voltage_v': round(sum(p['volt_mv'] for p in packs) / len(packs) / 1000.0, 2),
        'voltage_mv': round(sum(p['volt_mv'] for p in packs) / len(packs)),
        'vhigh_mv': int(max(p['vhigh_mv'] for p in packs)),
        'vlow_mv':  int(min(p['vlow_mv'] for p in packs)),
        'volt_st':  next((p['volt_st'] for p in packs if p['volt_st'] != 'Normal'), 'Normal'),
        'packs':    len(packs),
        'ts':       datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
    }

    print(f"EBox: SOC={avg_soc:.1f}%  P={total_w:.0f}W  I={total_a:.2f}A  ({len(packs)} Packs, ×{ebox_mult})")

    try:
        publish_mqtt(payload, VENUS_MQTT_HOST)
        print(f"MQTT {VENUS_MQTT_HOST} {MQTT_TOPIC}: ok")
    except Exception as e:
        print(f"MQTT publish error ({VENUS_MQTT_HOST}): {e}")

    try:
        publish_mqtt(payload)
        print(f"MQTT {MQTT_TOPIC}: {json.dumps(payload)}")
    except Exception as e:
        print(f"MQTT publish error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
