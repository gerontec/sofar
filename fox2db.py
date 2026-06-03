#!/usr/bin/env python3
import subprocess
import os
import math
import json
import datetime as dt
import zoneinfo
import time
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
VERSION = "v2.0-Py"
MAX_LOG_BYTES = 220 * 1024

STATE_TO_POWER = {0: 0, 1: 3000, 2: 3650, 3: 6650, 4: 3900, 5: 7100, 6: 7800, 7: 11400}
SORTED_STATES  = sorted(STATE_TO_POWER, key=lambda s: STATE_TO_POWER[s])

CONFIG = {
    'min_excess':                  1010,
    'max_grid_draw':               1500,
    'max_soc':                       99,
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
    'ebox_data':      '/tmp/ebox15k.txt',
    'inverter_csv':   '/tmp/inverter.csv',
    'ebox_script':    '/home/pi/python/ebox1arg.py',
    'ebyte_script':   '/home/pi/python/ebyte_ctrl.py',
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
# Format: (bedingung_fn, erzwungener_state, name)
# Werden NACH decide() angewendet — kein Code kann sie umgehen

def _hard_guards(soc):
    return [
        (lambda: soc > CONFIG['max_soc'],          0, "BATTERY_FULL_STOP"),
        (lambda: 0 <= soc < CONFIG['deep_discharge_lower'], 1, "CRITICAL_SOC_PROTECTION_ACTIVATE"),
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


def _db_decision_log(relay_st, final, trace, pcc, bat1, soc, excess, dc_pv, dc_exp):
    try:
        conn = _db_connect()
        cur  = conn.cursor()
        decision = trace.split("|")[0].split("(")[0].strip()[:64]
        cur.execute(
            "INSERT INTO pv_decision_log "
            "(ts,state_from,state_to,decision,detail,pcc_w,bat1_w,soc,excess_w,dc_pv_w,dc_expected_w) "
            "VALUES (NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (relay_st, final, decision, trace[:255],
             int(pcc), int(bat1), round(soc, 1), int(excess),
             int(dc_pv), int(dc_exp)))
        conn.commit(); conn.close()
    except Exception:
        pass

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

    # NaN guard
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


def read_ebox() -> Tuple[float, float]:
    try:
        subprocess.run(f"{PATHS['ebox_script']} pwr 1 > {PATHS['ebox_data']}",
                       shell=True, timeout=10)
    except Exception as e:
        _log(f"EBox update error: {e}")
    try:
        lines   = Path(PATHS['ebox_data']).read_text().splitlines()
        socs, current = [], 0.0
        for line in lines:
            line = line.strip().lstrip("b'").rstrip("'")
            if not line or line.startswith("Power") or line[0] not in "123":
                continue
            parts = line.split()
            if len(parts) > 2:
                try:
                    current += float(parts[2])
                except ValueError:
                    pass
            for p in parts:
                if "%" in p:
                    try:
                        socs.append(float(p.replace("%", "")))
                    except ValueError:
                        pass
        return current / 1000.0, (min(socs) if socs else -1.0)
    except Exception as e:
        _log(f"EBox read error: {e}")
        return 0.0, -1.0


def set_relay(state: int, reason: str = "") -> bool:
    script = PATHS['ebyte_script']
    prefix = "python3 " if script.endswith(".py") else ""
    try:
        ret = subprocess.run(f"{prefix}{script} {state}", shell=True,
                             timeout=10, capture_output=True, text=True)
        if ret.returncode == 0:
            _write(PATHS['relay_state'], state)
            _log(f"Relay OK: → {state} | {ret.stdout.strip()}")
            return True
        _log(f"Relay ERROR State {state}: {ret.stdout.strip()}")
    except subprocess.TimeoutExpired:
        _log(f"Relay timeout state {state}")
    return False


def pulse_do4(duration=3):
    script = PATHS['ebyte_script']
    try:
        proc = subprocess.Popen(["python3", script, "r4", "pulse", str(duration)])
        _log(f"DO4 Puls ausgelöst (PID={proc.pid})")
        _db_relay_event("pulse", "do4", 1, f"PCC>20kW PID={proc.pid}", duration)
    except OSError as e:
        _log(f"DO4 fork fehlgeschlagen: {e}")


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

def decide(soc, pcc, bat_cur, bat1, relay_st, prot) -> Tuple[int, str, float]:
    """
    Entscheidungslogik — dominanzgeordnet, erste passende Regel gewinnt.
    BATTERY_FULL_STOP und CRITICAL_SOC sind in HARD_GUARDS (nach decide).
    """
    ebox_eff = max(bat_cur * 2 * 53 + 1, STATE_TO_POWER.get(relay_st, 0)) if relay_st > 0 else 0
    excess   = pcc + ebox_eff + bat1

    # SOC unbekannt → aktuellen State halten
    if soc < 0:
        ebox_actual = (bat_cur * 2 * 53 + 1) if relay_st > 0 else 0
        excess = pcc + ebox_actual + bat1
        return relay_st, f"EBOX_SOC_UNKNOWN_HOLD", excess

    # Tiefentladeschutz aktiv
    if prot:
        if soc < CONFIG['deep_discharge_charge_target']:
            return 1, f"EMERGENCY_CHARGE_TO_{CONFIG['deep_discharge_charge_target']}% ({soc:.1f}%)", excess
        else:
            return 0, f"CHARGE_TARGET_REACHED ({soc:.1f}%>={CONFIG['deep_discharge_charge_target']}%)", excess

    # Ungenügender Überschuss
    if excess < CONFIG['min_excess']:
        return 0, f"INSUFFICIENT_EXCESS ({excess:.0f}W)", excess

    # Power Matching
    budget = excess + CONFIG['max_grid_draw']
    best   = max((s for s, p in STATE_TO_POWER.items() if p <= budget),
                 key=lambda s: STATE_TO_POWER[s], default=0)
    trace  = f"POWER_MATCHING (Excess: {excess:.0f}W, Budget: {budget:.0f}W)"

    # Ramp Limiting
    if best > relay_st:
        idx     = SORTED_STATES.index(relay_st) if relay_st in SORTED_STATES else 0
        next_st = SORTED_STATES[min(idx + 1, len(SORTED_STATES) - 1)]
        if best > next_st:
            trace += f" | RAMP_LIMITED ({best}->{next_st})"
            best = next_st

    return best, trace, excess


def apply_guards(best, soc, trace) -> Tuple[int, str]:
    """HARD_GUARDS — physikalische Invarianten, nach decide(), unüberwindbar."""
    for cond, state, name in _hard_guards(soc):
        if cond():
            if best != state:
                trace += f" | GUARD:{name}"
            return state, trace
    return best, trace


def apply_blocking(best, relay_st, pcc, bat1, stable, drop_rate, trace) -> Tuple[int, bool, str]:
    """Blocking Layer — verhindert zu schnelle Zustandsänderungen."""
    if best == relay_st:
        return relay_st, False, trace

    pwr_diff  = abs(STATE_TO_POWER[best] - STATE_TO_POWER[relay_st])
    direction = 'up' if STATE_TO_POWER[best] > STATE_TO_POWER[relay_st] else 'down'

    # EMERGENCY_FORCE — Blocking bypass bei schwerem Netzbezug
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

    # ── INPUT ──────────────────────────────────────────────────────────────
    mqtt_data = fetch_mqtt()
    if mqtt_data is None:
        _log("EMERGENCY SHUTDOWN: MQTT failed")
        set_relay(0, "MQTT failed")
        return

    bat_cur, soc  = read_ebox()
    wirkleist     = fetch_z2()
    raw_pcc       = mqtt_data['pcc']
    relay_st      = _read(PATHS['relay_state'])
    stable        = _read(PATHS['last_change'])
    prot          = _read(PATHS['deep_discharge']) == 1
    last_excess   = _read(PATHS['last_excess'], as_float=True)
    dc_expected   = DC.now()

    # Z2-Fallback wenn PCC=NaN
    if math.isnan(raw_pcc):
        pcc = abs(min(0.0, wirkleist))
        _log(f"PCC=NaN → Z2-Fallback: wirkleist={wirkleist:.0f}W → pcc={pcc:.0f}W")
    else:
        pcc = raw_pcc

    bat1     = mqtt_data['bat1']
    soc_bat1 = mqtt_data['soc_bat1']
    ebox_w   = bat_cur * 2 * 53

    # ── DECIDE ─────────────────────────────────────────────────────────────
    best, trace, excess = decide(soc, pcc, bat_cur, bat1, relay_st, prot)

    # HARD_GUARDS — physikalisch unüberwindbar
    best, trace = apply_guards(best, soc, trace)

    # Trend
    drop_rate    = (excess - last_excess) / 30.0 if last_excess > 0 else 0
    has_drop     = last_excess > 0
    _write(PATHS['last_excess'], f"{excess:.1f}")
    if has_drop and drop_rate != 0:
        _log(f"Trend: Excess {last_excess:.0f}W→{excess:.0f}W ({drop_rate:+.1f}W/s)")

    # Blocking
    final, changed, trace = apply_blocking(best, relay_st, pcc, bat1, stable, drop_rate, trace)

    # ── OUTPUT ─────────────────────────────────────────────────────────────
    new_stable = 0 if changed else stable + 1
    dc_delta   = dc_expected - (pcc + ebox_w + bat1)

    _log(f"Data: SOC2={soc:.1f}% SOC1={soc_bat1:.1f}% "
         f"PCC={pcc:.0f}W Z2={wirkleist:.0f}W "
         f"Bat1={bat1:.0f}W EBox={ebox_w:.0f}W "
         f"DC_exp={dc_expected:.0f}W DC_delta={dc_delta:+.0f}W "
         f"(State={relay_st}) Stable={new_stable}")
    _log(f"Result: State {final} (TRACE: {trace})")

    # MQTT publish
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
    })

    # Dateien
    _write(PATHS['last_change'], new_stable)
    _write(PATHS['inverter_csv'],
           f"{pcc:.0f},{bat1:.0f},{soc:.1f},{soc_bat1:.1f},{bat_cur:.1f},{final}\n")

    # Deep Discharge Protection (Hysterese)
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

    # DB Logging
    _db_decision_log(relay_st, final, trace, pcc, bat1, soc, excess, ebox_w, dc_expected)

    # Relay schalten
    if changed:
        _db_relay_event("state_change", "ebox", final, trace)
        set_relay(final)
    else:
        _write(PATHS['relay_state'], final)

    # PCC_OVER_20KW — Post-Guard (prio 1: höchste Dominanz)
    # SOC < 100 → State+1; SOC = 100 oder State bereits max → DO4
    if pcc > CONFIG['pcc_peak_threshold']:
        next_st = min(final + 1, 7)
        if soc < 100 and next_st > final:
            reason = f"PCC>20kW SOC={soc:.0f}%<100% State{final}→{next_st}"
            _log(f"PCC>20kW: {reason} (kein DO4)")
            _db_relay_event("state_change", "ebox", next_st, reason)
            set_relay(next_st)
        else:
            pulse_do4()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        import traceback
        _log(f"Fatal: {e}\n{traceback.format_exc()}")
