#!/usr/bin/env python3
# Hauptziel:  EBox (=Bat2) mit best_state laden (POWER_MATCHING).
# Nebenziel:  PCC-Einspeisung <20kW halten (PCC_OVER_20KW → State+1, DO4-Puls → Relais 4 schaltet WR2 ab).
# Takt:       Cron jede Minute.
#
# Geändert gegenüber .119-Version:
#   - read_ebox()  → liest SOC2 + Leistung aus pv_ebox2 (MariaDB), kein ebox1arg.py
#   - set_relay()  → MQTT waveshare/relay/1,2,3  statt ebyte_ctrl.py
#   - pulse_do4()  → MQTT waveshare/relay/4 On→sleep→Off statt ebyte_ctrl.py
#   - Startup:       sofar/auto {"ENABLE":0}  → ESP32-Eigenlogik deaktivieren
import subprocess
import os
import math
import json
import datetime as dt
import zoneinfo
import time
import threading
from typing import Tuple, Dict, Optional
from pathlib import Path

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import pymysql
from astral import LocationInfo as _LocationInfo
from astral.sun import elevation as _astral_elevation, azimuth as _astral_azimuth

# ═══════════════════════════════════════════════════════════════════════════
#                             KONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════
VERSION = "v2.9-Py"
MAX_LOG_BYTES = 220 * 1024

STATE_TO_POWER = {0: 0, 1: 3000, 2: 3650, 3: 6650, 4: 3900, 5: 7100, 6: 7800, 7: 11400}
SORTED_STATES  = sorted(STATE_TO_POWER, key=lambda s: STATE_TO_POWER[s])

CONFIG = {
    'min_excess':                  1010,
    'max_grid_draw':               1500,
    'max_soc':                      100,
    'hysteresis':                   505,
    'stabilization_cycles':           2,
    'emergency_import':            1020,
    'bat_discharge_threshold':     -220,
    'sweet_spot_pcc':               160,
    'sweet_spot_bat':              -310,
    'max_drop_rate':                -20,
    'deep_discharge_lower':           6,
    'deep_discharge_upper':           8,
    'deep_discharge_charge_target':   7,
    'pcc_peak_threshold':         20000,
}

PATHS = {
    'deep_discharge': '/tmp/deep_discharge_protection_active.txt',
    'log':            '/tmp/fox2db.log',
    'relay_state':    '/tmp/current_relay_state.txt',
    'last_change':    '/tmp/last_relay_change.txt',
    'last_excess':    '/tmp/last_excess.txt',
    'inverter_csv':   '/tmp/inverter.csv',
}

MQTT_CFG = {
    'broker':       'kellertreppe.fritz.box',
    'port':          1883,
    'topic':        'inverter/power_grid_exchange/json',
    'zaehl_topic':  'pv_zaehl2/#',
    'timeout':       43,
    'pub_topic':    'fox2db/state',
}

DB_CFG = {'host': '192.168.178.218', 'db': 'wagodb', 'user': 'gh', 'pw': 'a12345'}

# ═══════════════════════════════════════════════════════════════════════════
#                         DC-KLARHIMMEL-PROGNOSE
# ═══════════════════════════════════════════════════════════════════════════

class _DcForecast:
    _LAT, _LON = 47.6811, 11.5732
    _TZ = zoneinfo.ZoneInfo("Europe/Berlin")
    _ARRAYS      = [(25, 80, 27_854), (60, -5, 11_138)]
    _ARRAYS_EAST = [(41, -74, 19_852), (60, 90, 2_078), (32, 94, 2_378)]
    _KT = {1:.331,2:.402,3:.563,4:.838,5:.909,6:.880,
           7:.840,8:.820,9:.760,10:.600,11:.350,12:.134}

    def __init__(self):
        self._loc = _LocationInfo("Lenggries", "Germany", "Europe/Berlin", self._LAT, self._LON)

    def _cos_aoi(self, elev, az_sun_N, tilt, az_panel_S):
        e  = math.radians(elev)
        b  = math.radians(tilt)
        da = math.radians((az_sun_N - 180.0) - az_panel_S)
        return math.sin(e)*math.cos(b) + math.cos(e)*math.sin(b)*math.cos(da)

    def _calc(self, arrays, t):
        t_tz = t.replace(tzinfo=self._TZ) if t.tzinfo is None else t
        elev = _astral_elevation(self._loc.observer, t_tz)
        if elev <= 0:
            return 0.0
        az_N = _astral_azimuth(self._loc.observer, t_tz)
        am   = min(1.0 / math.sin(math.radians(elev)), 37.0)
        T    = 0.7 ** (am ** 0.678)
        kt   = self._KT.get(t.month, 0.60)
        return sum(p * T * kt * max(0.0, self._cos_aoi(elev, az_N, tilt, az))
                   for tilt, az, p in arrays)

    def now(self):
        t = dt.datetime.now(self._TZ).replace(second=0, microsecond=0)
        return self._calc(self._ARRAYS, t) + self._calc(self._ARRAYS_EAST, t)

    def peak_today(self, threshold=20_000):
        today = dt.date.today()
        best_w, best_t, window_end = 0.0, None, None
        for hour in range(5, 21):
            t = dt.datetime(today.year, today.month, today.day, hour, 0, tzinfo=self._TZ)
            w = self._calc(self._ARRAYS, t) + self._calc(self._ARRAYS_EAST, t)
            if w > best_w:
                best_w, best_t = w, t
            if w > threshold:
                window_end = t
        return best_w, best_t, best_w > threshold, window_end


DC = _DcForecast()

# ═══════════════════════════════════════════════════════════════════════════
#                         HARD GUARDS (physikalische Invarianten)
# ═══════════════════════════════════════════════════════════════════════════

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

def _ladesperre_release_db(reason: str):
    _db_relay_event("schlechtwetter", "ladesperre", 0, reason)

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


def _hard_guards(soc, ladesperre=False):
    return [
        (lambda: ladesperre,                                  0, "LADESPERRE_BIS_PCC_20KW"),
        (lambda: soc >= CONFIG['max_soc'],                     0, "BATTERY_FULL_STOP"),
        (lambda: 0 <= soc < CONFIG['deep_discharge_lower'],   1, "CRITICAL_SOC_PROTECTION_ACTIVATE"),
    ]

# ═══════════════════════════════════════════════════════════════════════════
#                         BLOCKING RULES
# ═══════════════════════════════════════════════════════════════════════════

def _blocking_rules(pcc, bat1, stable, drop_rate, pwr_diff):
    return [
        ('SWEET_SPOT_HOLD',
         lambda: abs(pcc) < CONFIG['sweet_spot_pcc'] and bat1 > CONFIG['sweet_spot_bat'],
         'up'),
        ('TREND_BLOCK',
         lambda: drop_rate < CONFIG['max_drop_rate'] and drop_rate != 0,
         'up'),
        ('BAT_GUARD_BLOCK',
         lambda: bat1 < CONFIG['bat_discharge_threshold'],
         'up'),
        ('STABILIZING',
         lambda: stable < CONFIG['stabilization_cycles'],
         'down'),
        ('HYSTERESIS',
         lambda: pwr_diff < CONFIG['hysteresis'],
         'down'),
    ]

# ═══════════════════════════════════════════════════════════════════════════
#                         HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def _log(msg):
    ts   = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts} {VERSION}] {msg}\n"
    path = Path(PATHS['log'])
    mode = "w" if path.exists() and path.stat().st_size >= MAX_LOG_BYTES else "a"
    try:
        path.open(mode).write(line)
    except OSError as e:
        print(f"Log Error: {e}")


def _read(path, default=0, as_float=False):
    try:
        v = Path(path).read_text().strip()
        return (float if as_float else int)(v) if v else default
    except Exception:
        return default


def _write(path, content):
    try:
        Path(path).write_text(str(content))
    except OSError as e:
        _log(f"Write error {path}: {e}")


def _db_connect():
    return pymysql.connect(host=DB_CFG['host'], database=DB_CFG['db'],
                           user=DB_CFG['user'], password=DB_CFG['pw'],
                           connect_timeout=3)


def _db_relay_event(action, relay, state_new, reason, duration_s=None):
    try:
        conn = _db_connect()
        cur  = conn.cursor()
        cur.execute(
            "INSERT INTO pv_relay_events (ts,action,relay,state_new,reason,duration_s) "
            "VALUES (NOW(),%s,%s,%s,%s,%s)",
            (action, relay, state_new, reason[:255], duration_s))
        cur.execute("DELETE FROM pv_relay_events WHERE relay=%s "
                    "AND ts < NOW() - INTERVAL 30 DAY", (relay,))
        conn.commit(); conn.close()
    except Exception as e:
        _log(f"DB relay_event error: {e}")


def _db_decision_log(relay_st, final, trace, pcc, bat1, soc, excess, ebox, dc_exp, dc_delta):
    try:
        conn = _db_connect()
        cur  = conn.cursor()
        decision = trace.split("|")[0].split("(")[0].strip()[:64]
        cur.execute(
            "INSERT INTO pv_decision_log "
            "(ts,version,state_from,state_to,decision,detail,pcc_w,bat1_w,soc,excess_w,ebox_w,dc_expected_w,dc_delta_w) "
            "VALUES (NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (VERSION, relay_st, final, decision, trace[:255],
             int(pcc), int(bat1), round(soc, 1), int(excess),
             int(ebox), int(dc_exp), int(dc_delta)))
        conn.commit(); conn.close()
    except Exception as e:
        _log(f"DB decision_log error: {e}")

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

    client = mqtt.Client(CallbackAPIVersion.VERSION2)
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

    if math.isnan(data['pcc']):
        data['pcc'] = float('nan')

    _log(f"MQTT Received: PCC={data['pcc']:.0f}W, Bat1={data['bat1']:.0f}W, "
         f"SOC_Bat1={data['soc_bat1']:.1f}%")
    return data


def fetch_z2() -> float:
    received = []

    def on_message(_c, _u, msg):
        try:
            received.append(float(json.loads(msg.payload.decode()).get("wirkleist", 0.0)))
        except Exception:
            pass

    client = mqtt.Client(client_id="fox2db_z2", clean_session=True)
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


def read_ebox() -> Tuple[float, float, float]:
    """Liest Bat2 (EBox) direkt aus MariaDB pv_ebox2.
    Gibt (current_a, soc_pct, power_w) zurück; soc=-1.0 bei DB-Fehler."""
    try:
        conn = _db_connect()
        cur  = conn.cursor()
        cur.execute("SELECT Coulomb, Volt, Curr FROM pv_ebox2 "
                    "WHERE ts >= NOW() - INTERVAL 3 MINUTE "
                    "ORDER BY ts DESC LIMIT 1")
        row = cur.fetchone()
        conn.close()
        if row is None:
            _log("EBox DB: kein aktueller Datensatz (<3min)")
            return 0.0, -1.0, 0.0
        soc_pct  = float(row[0])             # Coulomb = SOC %
        volt_mv  = float(row[1])             # mV
        curr_ma  = float(row[2])             # mA
        power_w  = volt_mv * curr_ma / 1_000_000.0
        current_a = curr_ma / 1000.0
        _log(f"EBox DB: SOC2={soc_pct:.1f}%  P={power_w:.0f}W  I={current_a:.2f}A")
        return current_a, soc_pct, power_w
    except Exception as e:
        _log(f"EBox DB error: {e}")
        return 0.0, -1.0, 0.0


def _mqtt_relay_pub(payloads: dict, client_id="fox2db_relay"):
    """Einmaliger MQTT-Connect, mehrere Topics publishen, disconnect."""
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
    """Schaltet EBox-Relais 1-3 über Waveshare MQTT.
    Bit 0 → relay/1 (CH1), Bit 1 → relay/2, Bit 2 → relay/3.
    Deaktiviert ESP32-Eigenlogik (sofar/auto ENABLE:0) vor dem Schalten."""
    payloads = {
        'sofar/auto':        '{"ENABLE":0}',
        'waveshare/relay/1': f'{{"v":{1 if state & 1 else 0}}}',
        'waveshare/relay/2': f'{{"v":{1 if state & 2 else 0}}}',
        'waveshare/relay/3': f'{{"v":{1 if state & 4 else 0}}}',
    }
    _mqtt_relay_pub(payloads)
    _write(PATHS['relay_state'], state)
    _log(f"Relay MQTT: State→{state} (CH1={state&1} CH2={1 if state&2 else 0} CH3={1 if state&4 else 0}) | {reason}")
    return True


def pulse_do4(duration=3):
    """WR2-Abregelung: relay/4 ON → sleep(duration) → OFF (non-blocking Thread)."""
    def _pulse():
        _mqtt_relay_pub({'waveshare/relay/4': '{"v":1}'}, client_id="fox2db_do4_on")
        time.sleep(duration)
        _mqtt_relay_pub({'waveshare/relay/4': '{"v":0}'}, client_id="fox2db_do4_off")
        _log(f"DO4 Puls abgeschlossen ({duration}s)")
    t = threading.Thread(target=_pulse, daemon=True)
    t.start()
    _log(f"DO4 Puls gestartet ({duration}s, relay/4)")
    _db_relay_event("pulse", "do4", 1, f"PCC>20kW pulse={duration}s", duration)


def publish_ebox(soc, ebox_w):
    client = mqtt.Client(client_id="fox2db_ebox", clean_session=True)
    try:
        client.connect(MQTT_CFG['broker'], MQTT_CFG['port'], keepalive=10)
        payload = json.dumps({"soc": round(soc, 1), "power": round(ebox_w)}, separators=(',', ':'))
        r = client.publish("ebox/status", payload, qos=0, retain=True)
        r.wait_for_publish(timeout=4)
    except Exception as e:
        _log(f"ebox/status publish error: {e}")
    finally:
        try: client.disconnect()
        except Exception: pass


def publish_mqtt(payload: dict):
    json_str = json.dumps(payload, separators=(',', ':'))
    client = mqtt.Client(client_id="fox2db_pub", clean_session=True)
    try:
        client.connect(MQTT_CFG['broker'], MQTT_CFG['port'], keepalive=10)
        result = client.publish(MQTT_CFG['pub_topic'], json_str, qos=0, retain=True)
        result.wait_for_publish(timeout=4)
        _log(f"MQTT-Publish: {len(json_str)} Bytes → {MQTT_CFG['pub_topic']}")
    except Exception as e:
        _log(f"MQTT-Publish error: {e}")
    finally:
        client.disconnect()

# ═══════════════════════════════════════════════════════════════════════════
#                         DECISION LAYER
# ═══════════════════════════════════════════════════════════════════════════

def decide(soc, pcc, ebox_w, bat1, relay_st, prot) -> Tuple[int, str, float]:
    ebox_eff = max(ebox_w, STATE_TO_POWER.get(relay_st, 0)) if relay_st > 0 else 0
    excess   = pcc + ebox_eff + bat1

    if soc < 0:
        ebox_actual = ebox_w if relay_st > 0 else 0
        excess = pcc + ebox_actual + bat1
        return relay_st, "EBOX_SOC_UNKNOWN_HOLD", excess

    if pcc > CONFIG['pcc_peak_threshold'] and soc < CONFIG['max_soc']:
        next_st = min(relay_st + 1, 7)
        if next_st > relay_st:
            return next_st, f"PCC_OVER_20KW (SOC={soc:.0f}% State{relay_st}→{next_st})", excess

    if prot:
        if soc < CONFIG['deep_discharge_charge_target']:
            return 1, f"EMERGENCY_CHARGE_TO_{CONFIG['deep_discharge_charge_target']}% ({soc:.1f}%)", excess
        else:
            return 0, f"CHARGE_TARGET_REACHED ({soc:.1f}%>={CONFIG['deep_discharge_charge_target']}%)", excess

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


def apply_guards(best, soc, trace, ladesperre=False) -> Tuple[int, str, bool]:
    for cond, state, name in _hard_guards(soc, ladesperre):
        if cond():
            if best != state:
                trace += f" | GUARD:{name}"
            return state, trace, True
    return best, trace, False


def apply_blocking(best, relay_st, pcc, bat1, stable, drop_rate, trace) -> Tuple[int, bool, str]:
    if best == relay_st:
        return relay_st, False, trace

    pwr_diff  = abs(STATE_TO_POWER[best] - STATE_TO_POWER[relay_st])
    direction = 'up' if STATE_TO_POWER[best] > STATE_TO_POWER[relay_st] else 'down'

    if pcc < -CONFIG['emergency_import'] and direction == 'down':
        trace += f" | EMERGENCY_FORCE (Import={pcc:.0f}W)"
        return best, True, trace

    for name, check, applies in _blocking_rules(pcc, bat1, stable, drop_rate, pwr_diff):
        if applies in (direction, 'both') and check():
            trace += f" | {name}"
            return relay_st, False, trace

    return best, True, trace

# ═══════════════════════════════════════════════════════════════════════════
#                              MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    _log("--- Start Cycle ---")

    # ESP32-Eigenlogik deaktivieren (läuft auch nach ESP-Neustart wieder hoch)
    _mqtt_relay_pub({'sofar/auto': '{"ENABLE":0}'}, client_id="fox2db_autooff")

    # ── INPUT ──────────────────────────────────────────────────────────────
    mqtt_data = fetch_mqtt()
    if mqtt_data is None:
        _log("EMERGENCY SHUTDOWN: MQTT failed")
        set_relay(0, "MQTT failed")
        return

    bat_cur, soc, ebox_w = read_ebox()
    publish_ebox(soc, ebox_w)
    wirkleist     = fetch_z2()
    raw_pcc       = mqtt_data['pcc']
    relay_st      = _read(PATHS['relay_state'])
    stable        = _read(PATHS['last_change'])
    prot          = _read(PATHS['deep_discharge']) == 1
    last_excess   = _read(PATHS['last_excess'], as_float=True)
    dc_expected                         = DC.now()
    _peak_w, _peak_t, _has_peak, _win_end = DC.peak_today()
    _peak_str = (f"DC_peak={_peak_w/1000:.1f}kW@{_peak_t.strftime('%H:%M')}"
                 f" win_end={_win_end.strftime('%H:%M')}") if _has_peak and _peak_t else "DC_peak=<20kW"

    if math.isnan(raw_pcc):
        pcc = abs(min(0.0, wirkleist))
        _log(f"PCC=NaN → Z2-Fallback: wirkleist={wirkleist:.0f}W → pcc={pcc:.0f}W")
    else:
        pcc = raw_pcc

    bat1     = mqtt_data['bat1']
    soc_bat1 = mqtt_data['soc_bat1']
    dc_delta = dc_expected - (pcc + ebox_w + bat1)
    pcc_avg  = _pcc_avg_10min()

    # ── LADESPERRE-Zustandsmaschine ─────────────────────────────────────────
    events = _ladesperre_events_today()
    if events is None:
        ladesperre = False
    else:
        do4_today, weather_rel, goodweather_rebl = events
        ratio = None
        if pcc_avg is not None and dc_expected > 5000:
            ratio = (dc_expected - (pcc_avg + ebox_w + bat1)) / dc_expected
        now_dt     = dt.datetime.now(DC._TZ)
        peak_ahead = _has_peak and _win_end is not None and now_dt < _win_end

        if do4_today:
            ladesperre = False
        elif goodweather_rebl:
            ladesperre = peak_ahead
            if not peak_ahead:
                _log("GUTWETTER-REBLOCK beendet — Peak-Fenster vorbei, Restladung frei")
        elif weather_rel:
            ladesperre = False
            if peak_ahead and dc_expected > 10000 and ratio is not None and ratio < 0.4:
                reason = f"Gutwetter zurueck PCC-avg={pcc_avg:.0f}W ratio={ratio:.0%}"
                _db_relay_event("gutwetter", "ladesperre", 1, reason)
                _log(f"GUTWETTER-REBLOCK — {reason} — Headroom fuer Peak")
                ladesperre = True
        else:
            ladesperre = True
            if ratio is not None:
                if ratio > 0.8:
                    reason = f"Schlechtwetter PCC-avg={pcc_avg:.0f}W ratio={ratio:.0%}"
                    _ladesperre_release_db(reason)
                    _log(f"LADESPERRE aufgehoben — {reason} > 80%")
                    ladesperre = False
                else:
                    _log(f"LADESPERRE aktiv — warte auf DO4-Peak (PCC-avg={pcc_avg:.0f}W ratio={ratio:.0%})")
            else:
                _log("LADESPERRE aktiv — warte auf DO4-Peak (PCC-Avg noch aufbauend)")

    # ── DECIDE ─────────────────────────────────────────────────────────────
    best, trace, excess = decide(soc, pcc, ebox_w, bat1, relay_st, prot)
    best, trace, guard_fired = apply_guards(best, soc, trace, ladesperre)

    drop_rate    = (excess - last_excess) / 30.0 if last_excess > 0 else 0
    has_drop     = last_excess > 0
    _write(PATHS['last_excess'], f"{excess:.1f}")
    if has_drop and drop_rate != 0:
        _log(f"Trend: Excess {last_excess:.0f}W→{excess:.0f}W ({drop_rate:+.1f}W/s)")

    if guard_fired:
        final, changed = best, (best != relay_st)
    else:
        final, changed, trace = apply_blocking(best, relay_st, pcc, bat1, stable, drop_rate, trace)

    # ── OUTPUT ─────────────────────────────────────────────────────────────
    new_stable = 0 if changed else stable + 1

    _log(f"Data: SOC2={soc:.1f}% SOC1={soc_bat1:.1f}% "
         f"PCC={pcc:.0f}W Z2={wirkleist:.0f}W "
         f"Bat1={bat1:.0f}W EBox={ebox_w:.0f}W "
         f"DC_exp={dc_expected:.0f}W DC_delta={dc_delta:+.0f}W "
         f"{_peak_str} (State={relay_st}) Stable={new_stable}")
    _log(f"Result: State {final} (TRACE: {trace})")

    publish_mqtt({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "version": VERSION,
        "soc_bat2": round(soc, 1),
        "soc_bat1": round(soc_bat1, 1),
        "pcc": round(pcc),
        "bat1": round(bat1),
        "ebox": round(ebox_w),
        "state": final,
        "state_before": relay_st,
        "stable": new_stable,
        "excess": round(excess),
        "drop_rate": round(drop_rate, 1) if has_drop and drop_rate != 0 else None,
        "trace": trace,
        "deep_discharge_active": prot,
        "need_downward_regulation": pcc > CONFIG['pcc_peak_threshold'] and (
            not trace.startswith("PCC_OVER_20KW") or ladesperre),
        "ladesperre": ladesperre,
        "dc_peak_time":   _peak_t.strftime('%H:%M') if _has_peak and _peak_t else None,
        "dc_window_end":  _win_end.strftime('%H:%M') if _has_peak and _win_end else None,
    })

    _write(PATHS['last_change'], new_stable)
    _write(PATHS['inverter_csv'],
           f"{pcc:.0f},{bat1:.0f},{soc:.1f},{soc_bat1:.1f},{bat_cur:.1f},{final}\n")

    old_prot = prot
    if soc >= 0:
        if soc < CONFIG['deep_discharge_lower']:
            if not old_prot:
                _log(f"DEEP_DISCHARGE_PROTECTION ACTIVATED at {soc:.1f}%")
            _write(PATHS['deep_discharge'], 1)
        elif soc >= CONFIG['deep_discharge_upper']:
            if old_prot:
                _log(f"DEEP_DISCHARGE_PROTECTION DEACTIVATED at {soc:.1f}%")
            _write(PATHS['deep_discharge'], 0)

    if dc_expected > 0:
        _db_decision_log(relay_st, final, trace, pcc, bat1, soc, excess, ebox_w, dc_expected, dc_delta)

    if changed:
        _db_relay_event("state_change", "ebox", final, trace)
        set_relay(final, trace)
    else:
        _write(PATHS['relay_state'], final)

    need_downward_regulation = pcc > CONFIG['pcc_peak_threshold'] and (
        not trace.startswith("PCC_OVER_20KW") or ladesperre
    )
    if need_downward_regulation:
        if ladesperre:
            _log("LADESPERRE aufgehoben — erster DO4-Trigger heute")
        pulse_do4()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        import traceback
        _log(f"Fatal: {e}\n{traceback.format_exc()}")
