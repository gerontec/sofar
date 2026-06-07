#!/usr/bin/env python3
"""
soyo_mqtt.py  —  Soyo-Leistungsberechnung aus fox2db/state → soyo/set
Ablösung von soyo1min.py (RS485-direkt) durch MQTT→Waveshare→RS485

Logik (analog soyo1min.py):
  state != 0   → 0W  (EBox aktiv, kein Soyo)
  soc_bat2 < 9 → 0W  (Entladeschutz)
  pcc > 100    → min(pcc*1.01, MAX_W)  (Netz-Import kompensieren)
  pcc < -200   → 0W  (PV-Überschuss, kein Bedarf)
  default      → 390W (Nacht) / 10W (Tag)
"""

import json
import time
import sys
from datetime import datetime
import paho.mqtt.client as mqtt

MQTT_BROKER     = '192.168.178.218'
MQTT_PORT       = 1883
BAT2_SOC_MIN    = 9      # Entladeschutz Bat2 [%]
MAX_SOYO_W      = 600    # Soyo Hardware-Maximum [W]
NIGHT_W         = 390    # Basis-Leistung nachts [W]
DAY_IDLE_W      = 10     # Basis-Leistung tags [W]
PUBLISH_EVERY   = 25     # s  (<30s Stale-Safety im ESP32)
STALE_TIMEOUT   = 90     # s  — fox2db/state älter als das → 0W senden

_state   = {}
_state_t = 0.0


def calc_soyo(d):
    state    = d.get('state', 0)
    soc_bat2 = d.get('soc_bat2', 100)
    pcc      = d.get('pcc', 0)   # W, positiv = Import, negativ = Export

    if state != 0:
        return 0, f"state={state}≠0 (EBox aktiv)"

    if soc_bat2 < BAT2_SOC_MIN:
        return 0, f"soc_bat2={soc_bat2:.1f}% < {BAT2_SOC_MIN}%"

    if pcc > 100:
        w = min(int(pcc * 1.01), MAX_SOYO_W)
        return w, f"import={pcc}W → soyo={w}W"

    if pcc < -200:
        return 0, f"export={pcc}W → soyo=0"

    h = datetime.now().hour
    w = NIGHT_W if (h < 6 or h >= 20) else DAY_IDLE_W
    return w, f"balanced pcc={pcc}W, {'Nacht' if (h<6 or h>=20) else 'Tag'} → {w}W"


def on_message(client, userdata, msg):
    global _state, _state_t
    try:
        _state   = json.loads(msg.payload.decode())
        _state_t = time.monotonic()
    except Exception as e:
        print(f"[ERR] JSON: {e}", flush=True)


def main():
    mqttc = mqtt.Client(client_id='soyo_mqtt_calc')
    mqttc.on_message = on_message
    mqttc.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqttc.subscribe('fox2db/state', qos=0)
    mqttc.loop_start()

    print(f"[Start] soyo_mqtt  broker={MQTT_BROKER}  interval={PUBLISH_EVERY}s", flush=True)

    last_pub = 0.0
    while True:
        now = time.monotonic()
        if now - last_pub >= PUBLISH_EVERY:
            stale = (now - _state_t) > STALE_TIMEOUT if _state_t else True
            if stale:
                w, reason = 0, f"fox2db/state stale >{STALE_TIMEOUT}s"
            else:
                w, reason = calc_soyo(_state)
            mqttc.publish('soyo/set', json.dumps({"W": w}), qos=0)
            ts = _state.get('ts', '?') if _state else '—'
            print(f"[{datetime.now().strftime('%H:%M:%S')}] soyo/set={w}W  {reason}  ts={ts}", flush=True)
            last_pub = now
        time.sleep(1)


if __name__ == '__main__':
    main()
