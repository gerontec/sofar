#!/usr/bin/env python3
# fox2dbEasy.py — vereinfachte Version ohne EBox-Messung.
# EBox-Leistung wird theoretisch aus STATE_TO_POWER[relay_state] berechnet.
# Kein DB-Zugriff für EBox, kein MQTT ebox/pwr.
# Takt: Cron jede Minute.
import math
import json
import datetime as dt
import zoneinfo
import time
from typing import Tuple, Dict, Optional
from pathlib import Path

import paho.mqtt.client as mqtt
import pymysql
from astral import LocationInfo as _LocationInfo
from astral.sun import elevation as _astral_elevation, azimuth as _astral_azimuth

# ═══════════════════════════════════════════════════════════════════════════
#                             KONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════
VERSION = "v1.0-Easy"
MAX_LOG_BYTES = 100 * 1024

STATE_TO_POWER = {0: 0, 1: 3000, 2: 3650, 3: 6650, 4: 3900, 5: 7100, 6: 7800, 7: 11400}
SORTED_STATES  = sorted(STATE_TO_POWER, key=lambda s: STATE_TO_POWER[s])

CONFIG = {
    'min_excess':          1010,
    'max_grid_draw':       1500,
    'max_soc':              100,
    'hysteresis':           505,
    'stabilization_cycles':   2,
    'emergency_import':    1020,
    'pcc_peak_threshold': 20000,
}

PATHS = {
    'log':          '/tmp/fox2dbEasy.log',
    'relay_state':  '/tmp/current_relay_state_easy.txt',
    'last_change':  '/tmp/last_relay_change_easy.txt',
}

MQTT_CFG = {
    'broker':      '192.168.178.218',
    'port':         1883,
    'topic':       'inverter/power_grid_exchange/json',
    'zaehl_topic': 'pv_zaehl2/#',
    'timeout':      43,
    'pub_topic':   'fox2db/easy/state',
}

DB_CFG = {'host': '192.168.178.218', 'db': 'wagodb', 'user': 'gh', 'pw': 'a12345'}

# ═══════════════════════════════════════════════════════════════════════════
#                         DC-KLARHIMMEL-PROGNOSE
# ═══════════════════════════════════════════════════════════════════════════

class _DcForecast:
    _LAT, _LON = 47.6811, 11.5732
    _TZ = zoneinfo.ZoneInfo("Europe/Berlin")
    _ARRAYS = [(25, 80, 27_854), (60, -5, 11_138)]

    def now(self) -> float:
        loc = _LocationInfo(latitude=self._LAT, longitude=self._LON)
        now = dt.datetime.now(self._TZ)
        el  = _astral_elevation(loc.observer, now)
        if el <= 0:
            return 0.0
        el_r = math.radians(el)
        total = 0.0
        for tilt, azim, wp in self._ARRAYS:
            az_r = math.radians(_astral_azimuth(loc.observer, now) - azim)
            ti_r = math.radians(tilt)
            cos_i = (math.sin(el_r) * math.cos(ti_r)
                     + math.cos(el_r) * math.sin(ti_r) * math.cos(az_r))
            total += max(0, cos_i) * wp
        return round(total * 0.78)

DC = _DcForecast()

# ═══════════════════════════════════════════════════════════════════════════
#                         HILFSFUNKTIONEN
# ═══════════════════════════════════════════════════════════════════════════

def _log(msg: str):
    ts  = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts} {VERSION}] {msg}\n"
    print(line, end='')
    log = Path(PATHS['log'])
    if log.exists() and log.stat().st_size > MAX_LOG_BYTES:
        log.write_text('')
    with open(PATHS['log'], 'a') as f:
        f.write(line)

def _read(path, as_float=False):
    try:
        v = Path(path).read_text().strip()
        return float(v) if as_float else int(v)
    except Exception:
        return 0.0 if as_float else 0

def _write(path, value):
    Path(path).write_text(str(value))

def _db_connect():
    return pymysql.connect(host=DB_CFG['host'], db=DB_CFG['db'],
                           user=DB_CFG['user'], password=DB_CFG['pw'],
                           connect_timeout=8)

# ═══════════════════════════════════════════════════════════════════════════
#                         INPUT LAYER
# ═══════════════════════════════════════════════════════════════════════════

def fetch_mqtt() -> Optional[Dict]:
    data     = {}
    received = False

    def on_message(client, userdata, msg):
        nonlocal data, received
        try:
            j = json.loads(msg.payload.decode())
            data = {
                'pcc':      float(j.get('ActivePower_PCC_Total') or 0) * 1000,
                'bat1':     float(j.get('Power_Bat1')            or 0) * 1000,
                'soc_bat1': float(j.get('SOC_Bat1')              or 0),
            }
            received = True
            client.disconnect()
        except Exception as e:
            _log(f"MQTT parse error: {e}")

    client = mqtt.Client(client_id="fox2dbeasy_z1", clean_session=True)
    client.on_message = on_message
    try:
        client.connect(MQTT_CFG['broker'], MQTT_CFG['port'], 60)
        client.subscribe(MQTT_CFG['topic'])
        client.loop_start()
        deadline = time.monotonic() + MQTT_CFG['timeout']
        while not received and time.monotonic() < deadline:
            time.sleep(0.1)
        client.loop_stop()
        client.disconnect()
    except Exception as e:
        _log(f"MQTT error: {e}")
        return None

    if not received:
        _log(f"CRITICAL: MQTT Timeout after {MQTT_CFG['timeout']}s")
        return None

    _log(f"MQTT: PCC={data['pcc']:.0f}W  Bat1={data['bat1']:.0f}W  SOC1={data['soc_bat1']:.1f}%")
    return data


def fetch_z2() -> float:
    received = []

    def on_message(_c, _u, msg):
        try:
            received.append(float(json.loads(msg.payload.decode()).get("wirkleist", 0.0)))
        except Exception:
            pass

    client = mqtt.Client(client_id="fox2dbeasy_z2", clean_session=True)
    client.on_message = on_message
    try:
        client.connect(MQTT_CFG['broker'], MQTT_CFG['port'], keepalive=10)
        client.subscribe(MQTT_CFG['zaehl_topic'], qos=0)
        client.loop_start()
        deadline = time.monotonic() + 3.0
        while not received and time.monotonic() < deadline:
            time.sleep(0.05)
        client.loop_stop()
        client.disconnect()
    except Exception:
        pass
    return received[0] if received else 0.0

# ═══════════════════════════════════════════════════════════════════════════
#                         RELAY CONTROL
# ═══════════════════════════════════════════════════════════════════════════

def _mqtt_relay_pub(payloads: dict, client_id="fox2dbeasy_relay"):
    client = mqtt.Client(client_id=client_id, clean_session=True)
    try:
        client.connect(MQTT_CFG['broker'], MQTT_CFG['port'], keepalive=10)
        for topic, payload in payloads.items():
            r = client.publish(topic, payload, qos=0)
            r.wait_for_publish(timeout=3)
    except Exception as e:
        _log(f"MQTT relay pub error: {e}")
    finally:
        try: client.disconnect()
        except Exception: pass


def set_relay(state: int, reason: str = "") -> bool:
    payloads = {
        'sofar/auto':        '{"ENABLE":0}',
        'waveshare/relay/1': f'{{"v":{1 if state & 1 else 0}}}',
        'waveshare/relay/2': f'{{"v":{1 if state & 2 else 0}}}',
        'waveshare/relay/3': f'{{"v":{1 if state & 4 else 0}}}',
    }
    _mqtt_relay_pub(payloads)
    _write(PATHS['relay_state'], state)
    _log(f"Relay: State→{state} (CH1={state&1} CH2={1 if state&2 else 0} CH3={1 if state&4 else 0}) | {reason}")
    return True


def publish_state(state: int, pcc: float, bat1: float, ebox_w: float, excess: float, trace: str):
    payload = json.dumps({
        "version": VERSION,
        "state":   state,
        "pcc":     round(pcc),
        "bat1":    round(bat1),
        "ebox":    round(ebox_w),
        "excess":  round(excess),
        "trace":   trace,
        "ts":      dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
    }, separators=(',', ':'))
    client = mqtt.Client(client_id="fox2dbeasy_pub", clean_session=True)
    try:
        client.connect(MQTT_CFG['broker'], MQTT_CFG['port'], keepalive=10)
        client.publish(MQTT_CFG['pub_topic'], payload, qos=0, retain=True)
        _log(f"Published → {MQTT_CFG['pub_topic']}: {len(payload)}B")
    except Exception as e:
        _log(f"Publish error: {e}")
    finally:
        try: client.disconnect()
        except Exception: pass

# ═══════════════════════════════════════════════════════════════════════════
#                         ENTSCHEIDUNGSLOGIK
# ═══════════════════════════════════════════════════════════════════════════

def decide(pcc: float, ebox_w: float, bat1: float, relay_st: int) -> Tuple[int, str, float]:
    """Wie fox2db.decide() aber ohne SOC-Guards (kein EBox-SOC verfügbar)."""
    excess = pcc + ebox_w + bat1

    if pcc < -CONFIG['emergency_import']:
        return 0, f"EMERGENCY_IMPORT (PCC={pcc:.0f}W)", excess

    if pcc > CONFIG['pcc_peak_threshold']:
        best = min(relay_st + 0, 7)  # state halten, kein Hochschalten
        return relay_st, f"PCC_OVER_20KW (PCC={pcc:.0f}W)", excess

    if excess < CONFIG['min_excess']:
        return 0, f"INSUFFICIENT_EXCESS ({excess:.0f}W)", excess

    budget = excess + CONFIG['max_grid_draw']
    best   = max((s for s, p in STATE_TO_POWER.items() if p <= budget),
                 key=lambda s: STATE_TO_POWER[s], default=0)
    trace  = f"POWER_MATCHING (Excess: {excess:.0f}W, Budget: {budget:.0f}W)"

    # Ramp: nur einen Schritt hoch
    if best > relay_st:
        idx     = SORTED_STATES.index(relay_st) if relay_st in SORTED_STATES else 0
        next_st = SORTED_STATES[min(idx + 1, len(SORTED_STATES) - 1)]
        if best > next_st:
            trace += f" | RAMP_LIMITED ({best}->{next_st})"
            best = next_st

    return best, trace, excess

# ═══════════════════════════════════════════════════════════════════════════
#                              MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    _log("--- Start Cycle ---")

    # ── INPUT ──────────────────────────────────────────────────────────────
    mqtt_data = fetch_mqtt()
    if mqtt_data is None:
        _log("EMERGENCY SHUTDOWN: MQTT failed")
        set_relay(0, "MQTT failed")
        return

    wirkleist = fetch_z2()
    raw_pcc   = mqtt_data['pcc']
    bat1      = mqtt_data['bat1']
    relay_st  = _read(PATHS['relay_state'])
    stable    = _read(PATHS['last_change'])

    if math.isnan(raw_pcc):
        pcc = abs(min(0.0, wirkleist))
        _log(f"PCC=NaN → Z2-Fallback: wirkleist={wirkleist:.0f}W → pcc={pcc:.0f}W")
    else:
        pcc = raw_pcc

    # ── THEORETISCHER EBOX-WERT ────────────────────────────────────────────
    # Kein Messwert — theoretischer Wert aus aktuell geschaltetem State
    ebox_w = float(STATE_TO_POWER.get(relay_st, 0))
    soc    = -1.0  # unbekannt
    dc_exp = DC.now()

    _log(f"Data: PCC={pcc:.0f}W  Bat1={bat1:.0f}W  EBox(theo)={ebox_w:.0f}W"
         f"  DC_exp={dc_exp:.0f}W  (State={relay_st}) Stable={stable}")

    # ── DECIDE ─────────────────────────────────────────────────────────────
    best, trace, excess = decide(pcc, ebox_w, bat1, relay_st)

    # Stabilisierung: Hochschalten erst nach N stabilen Zyklen
    if best > relay_st:
        if stable < CONFIG['stabilization_cycles']:
            trace += f" | STABILIZING ({stable}/{CONFIG['stabilization_cycles']})"
            best = relay_st

    _log(f"Result: State {best} (TRACE: {trace})")

    # ── APPLY ──────────────────────────────────────────────────────────────
    changed = (best != relay_st)
    if changed:
        _write(PATHS['last_change'], 0)
    else:
        _write(PATHS['last_change'], stable + 1)

    set_relay(best, trace)
    publish_state(best, pcc, bat1, ebox_w, excess, trace)


if __name__ == '__main__':
    main()
