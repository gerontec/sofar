#!/usr/bin/env python3
# fox2dbEasy.py — vereinfachte Version ohne EBox-Messung.
# EBox-Leistung wird theoretisch aus STATE_TO_POWER[relay_state] berechnet.
# --shadow  : Relais werden NICHT geschaltet (nur Entscheidung loggen/publishen)
# Takt: Cron jede Minute (via fox2db_wrapper.py).
import sys
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

SHADOW = '--shadow' in sys.argv

# ═══════════════════════════════════════════════════════════════════════════
VERSION = "v1.0-Easy"
MAX_LOG_BYTES = 100 * 1024

STATE_TO_POWER = {0: 0, 1: 3000, 2: 3650, 3: 6650, 4: 3900, 5: 7100, 6: 7800, 7: 11400}
SORTED_STATES  = sorted(STATE_TO_POWER, key=lambda s: STATE_TO_POWER[s])

CONFIG = {
    'min_excess':          1200,
    'max_grid_draw':       1200,
    'stabilization_cycles':   2,
    'emergency_import':    1020,
    'pcc_peak_threshold': 20000,
}

PATHS = {
    'log':         '/tmp/fox2dbEasy.log',
    'relay_state': '/tmp/current_relay_state_easy.txt',
    'last_change': '/tmp/last_relay_change_easy.txt',
    'result':      '/tmp/fox2dbEasy_result.txt',   # letzter State für Wrapper
}

DB_CFG = {'host': '192.168.178.218', 'db': 'wagodb', 'user': 'gh', 'pw': 'a12345'}

MQTT_CFG = {
    'broker':      '192.168.178.218',
    'port':         1883,
    'topic':       'inverter/power_grid_exchange/json',
    'zaehl_topic': 'pv_zaehl2/#',
    'timeout':      43,
    'pub_topic':   'fox2db/easy/state',
}

# ═══════════════════════════════════════════════════════════════════════════
#                         DC-KLARHIMMEL-PROGNOSE
# ═══════════════════════════════════════════════════════════════════════════

class _DcForecast:
    _LAT, _LON = 47.6811, 11.5732
    _TZ = zoneinfo.ZoneInfo("Europe/Berlin")
    _ARRAYS = [(25, 80, 27_854), (60, -5, 11_138)]

    def _calc(self, t: dt.datetime) -> float:
        loc = _LocationInfo(latitude=self._LAT, longitude=self._LON)
        el  = _astral_elevation(loc.observer, t)
        if el <= 0:
            return 0.0
        el_r = math.radians(el)
        total = 0.0
        for tilt, azim, wp in self._ARRAYS:
            az_r = math.radians(_astral_azimuth(loc.observer, t) - azim)
            ti_r = math.radians(tilt)
            cos_i = (math.sin(el_r) * math.cos(ti_r)
                     + math.cos(el_r) * math.sin(ti_r) * math.cos(az_r))
            total += max(0, cos_i) * wp
        return round(total * 0.78)

    def now(self) -> float:
        return self._calc(dt.datetime.now(self._TZ))

    def peak_today(self, threshold=20_000):
        today = dt.date.today()
        best_w, best_t, window_end = 0.0, None, None
        for hour in range(5, 21):
            t = dt.datetime(today.year, today.month, today.day, hour, 0,
                            tzinfo=self._TZ)
            w = self._calc(t)
            if w > best_w:
                best_w, best_t = w, t
            if w > threshold:
                window_end = t
        return best_w, best_t, best_w > threshold, window_end

DC = _DcForecast()

# ═══════════════════════════════════════════════════════════════════════════
#                         HILFSFUNKTIONEN
# ═══════════════════════════════════════════════════════════════════════════

def _log(msg: str):
    ts   = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts} {VERSION}{'|shadow' if SHADOW else ''}] {msg}\n"
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
                'load_sys': float(j.get('ActivePower_Load_Sys')  or 0) * 1000,
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

    _log(f"MQTT: PCC={data['pcc']:.0f}W  Bat1={data['bat1']:.0f}W")
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
#                         DB / LADESPERRE
# ═══════════════════════════════════════════════════════════════════════════

def _db_connect():
    return pymysql.connect(host=DB_CFG['host'], database=DB_CFG['db'],
                           user=DB_CFG['user'], password=DB_CFG['pw'],
                           connect_timeout=3)

def _db_relay_event(action, relay, state_new, reason):
    if SHADOW:
        _log(f"Shadow: DB relay_event unterdrückt: {action}/{relay} → {reason}")
        return
    try:
        conn = _db_connect()
        cur  = conn.cursor()
        cur.execute(
            "INSERT INTO pv_relay_events (ts,action,relay,state_new,reason) "
            "VALUES (NOW(),%s,%s,%s,%s)",
            (action, relay, state_new, reason[:255]))
        conn.commit(); conn.close()
    except Exception as e:
        _log(f"DB relay_event error: {e}")

def _ladesperre_events_today():
    try:
        conn = _db_connect()
        cur  = conn.cursor()
        cur.execute("SELECT action, relay FROM pv_relay_events "
                    "WHERE relay IN ('do4','ladesperre') AND DATE(ts) = CURDATE()")
        rows = cur.fetchall()
        conn.close()
        do4     = any(r[1] == 'do4'                                     for r in rows)
        weather = any(r[1] == 'ladesperre' and r[0] == 'schlechtwetter' for r in rows)
        reblock = any(r[1] == 'ladesperre' and r[0] == 'gutwetter'      for r in rows)
        return do4, weather, reblock
    except Exception:
        return None

def _pcc_avg_10min():
    try:
        conn = _db_connect()
        cur  = conn.cursor()
        cur.execute("SELECT AVG(pcc_w), COUNT(*) FROM pv_decision_log "
                    "WHERE ts >= NOW() - INTERVAL 10 MINUTE AND pcc_w IS NOT NULL")
        avg, count = cur.fetchone()
        conn.close()
        return float(avg) if avg is not None and count >= 3 else None
    except Exception:
        return None

def _ladesperre_state(pcc_avg, ebox_w, bat1, dc_expected) -> bool:
    _peak_w, _peak_t, _has_peak, _win_end = DC.peak_today()
    now_dt     = dt.datetime.now(DC._TZ)
    peak_ahead = _has_peak and _win_end is not None and now_dt < _win_end

    events = _ladesperre_events_today()
    if events is None:
        return False

    do4_today, weather_rel, goodweather_rebl = events
    ratio = None
    if pcc_avg is not None and dc_expected > 5000:
        ratio = (dc_expected - (pcc_avg + ebox_w + bat1)) / dc_expected

    if do4_today:
        return False
    elif goodweather_rebl:
        if not peak_ahead:
            _log("GUTWETTER-REBLOCK beendet — Peak-Fenster vorbei")
        return peak_ahead
    elif weather_rel:
        if peak_ahead and dc_expected > 10000 and ratio is not None and ratio < 0.4:
            reason = f"Gutwetter zurueck PCC-avg={pcc_avg:.0f}W ratio={ratio:.0%}"
            _db_relay_event("gutwetter", "ladesperre", 1, reason)
            _log(f"GUTWETTER-REBLOCK — {reason}")
            return True
        return False
    else:
        if ratio is not None:
            if ratio > 0.8:
                reason = f"Schlechtwetter PCC-avg={pcc_avg:.0f}W ratio={ratio:.0%}"
                _db_relay_event("schlechtwetter", "ladesperre", 0, reason)
                _log(f"LADESPERRE aufgehoben — {reason}")
                return False
            else:
                _log(f"LADESPERRE aktiv ratio={ratio:.0%}")
        else:
            _log("LADESPERRE aktiv — PCC-Avg noch aufbauend")
        return True

# ═══════════════════════════════════════════════════════════════════════════
#                         RELAY CONTROL
# ═══════════════════════════════════════════════════════════════════════════

def set_relay(state: int, reason: str = ""):
    if SHADOW:
        _log(f"Shadow: Relay→{state} unterdrückt | {reason}")
        return
    payloads = {
        'sofar/auto':        '{"ENABLE":0}',
        'waveshare/relay/1': f'{{"v":{1 if state & 1 else 0}}}',
        'waveshare/relay/2': f'{{"v":{1 if state & 2 else 0}}}',
        'waveshare/relay/3': f'{{"v":{1 if state & 4 else 0}}}',
    }
    client = mqtt.Client(client_id="fox2dbeasy_relay", clean_session=True)
    try:
        client.connect(MQTT_CFG['broker'], MQTT_CFG['port'], keepalive=10)
        for topic, payload in payloads.items():
            client.publish(topic, payload, qos=0).wait_for_publish(timeout=3)
    except Exception as e:
        _log(f"MQTT relay error: {e}")
    finally:
        try: client.disconnect()
        except Exception: pass
    _write(PATHS['relay_state'], state)
    _log(f"Relay: State→{state} | {reason}")


def publish_state(state: int, pcc: float, bat1: float, ebox_w: float,
                  excess: float, trace: str):
    payload = json.dumps({
        "version": VERSION, "state": state,
        "pcc": round(pcc), "bat1": round(bat1),
        "ebox": round(ebox_w), "excess": round(excess),
        "trace": trace,
        "ts": dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
    }, separators=(',', ':'))
    client = mqtt.Client(client_id="fox2dbeasy_pub", clean_session=True)
    try:
        client.connect(MQTT_CFG['broker'], MQTT_CFG['port'], keepalive=10)
        client.publish(MQTT_CFG['pub_topic'], payload, qos=0, retain=True)
    except Exception as e:
        _log(f"Publish error: {e}")
    finally:
        try: client.disconnect()
        except Exception: pass

# ═══════════════════════════════════════════════════════════════════════════
#                         ENTSCHEIDUNGSLOGIK
# ═══════════════════════════════════════════════════════════════════════════

def decide(pcc: float, ebox_w: float, bat1: float, relay_st: int) -> Tuple[int, str, float]:
    excess = pcc + ebox_w + bat1

    if pcc < -CONFIG['emergency_import']:
        return 0, f"EMERGENCY_IMPORT (PCC={pcc:.0f}W)", excess

    if pcc > CONFIG['pcc_peak_threshold']:
        return relay_st, f"PCC_OVER_20KW (PCC={pcc:.0f}W)", excess

    if excess < CONFIG['min_excess']:
        return 0, f"INSUFFICIENT_EXCESS ({excess:.0f}W)", excess

    budget = excess + CONFIG['max_grid_draw']
    best   = max((s for s, p in STATE_TO_POWER.items() if p <= budget),
                 key=lambda s: STATE_TO_POWER[s], default=0)
    trace  = f"POWER_MATCHING (Excess: {excess:.0f}W, Budget: {budget:.0f}W)"

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

    mqtt_data = fetch_mqtt()
    if mqtt_data is None:
        _log("SHUTDOWN: MQTT failed")
        if not SHADOW:
            set_relay(0, "MQTT failed")
        _write(PATHS['result'], '-1')
        return

    wirkleist = fetch_z2()
    raw_pcc   = mqtt_data['pcc']
    bat1      = mqtt_data['bat1']
    load_sys  = mqtt_data['load_sys']
    relay_st  = _read(PATHS['relay_state'])
    stable    = _read(PATHS['last_change'])

    if math.isnan(raw_pcc):
        pcc = abs(min(0.0, wirkleist))
        _log(f"PCC=NaN → Z2={wirkleist:.0f}W")
    else:
        pcc = raw_pcc

    ebox_w = float(STATE_TO_POWER.get(relay_st, 0))
    dc_exp = DC.now()

    _log(f"Data: PCC={pcc:.0f}W  Bat1={bat1:.0f}W  EBox(theo)={ebox_w:.0f}W"
         f"  Load={load_sys:.0f}W  DC={dc_exp:.0f}W  State={relay_st}  Stable={stable}")

    best, trace, excess = decide(pcc, ebox_w, bat1, relay_st)

    pcc_avg    = _pcc_avg_10min()
    ladesperre = _ladesperre_state(pcc_avg, ebox_w, bat1, dc_exp)
    if ladesperre and best != 0:
        trace += " | GUARD:LADESPERRE_BIS_PCC_20KW"
        best = 0

    if best > relay_st and stable < CONFIG['stabilization_cycles']:
        trace += f" | STABILIZING ({stable}/{CONFIG['stabilization_cycles']})"
        best = relay_st

    _log(f"Result: State {best} (TRACE: {trace})")

    # Ergebnis für Wrapper in Datei schreiben
    _write(PATHS['result'], best)

    changed = (best != relay_st)
    if not SHADOW:
        _write(PATHS['last_change'], 0 if changed else stable + 1)

    set_relay(best, trace)
    publish_state(best, pcc, bat1, ebox_w, excess, trace)


if __name__ == '__main__':
    main()
