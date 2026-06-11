#!/usr/bin/env python3
# fox2dbEasy.py — Faithful Python-Port von fox2db_logic.h (fox::step).
# Trifft dieselben Entscheidungen wie der Waveshare ESP32 (sofar_waveshare.yaml),
# der ebenfalls fox::step() aufruft.
#
#   decide() -> apply_guards() -> apply_blocking()
#   + DC-Klarhimmel-Forecast (NOAA-Sonnenstand) + RAM-Ladesperre-Zustandsmaschine.
#
# Inputs (wie ESP):  inverter/power_grid_exchange/json  (PCC, Bat1, SOC1)
#                    pv_zaehl2/#                        (Z2 wirkleist, PCC-Fallback)
#                    ebox/pwr  (retained)              (SOC2, EBox-Leistung)
# Output:            fox2db/easy/state  (state + trace — Gründe als Debug-Feature!)
#
# Zustand (fox::State) wird über Cron-Zyklen in /tmp/fox2dbEasy_state.json gehalten
# und IMMER fortgeschrieben (auch im Shadow) — sonst kann die Zustandsmaschine
# nicht wie auf dem ESP evolvieren.
#
# --shadow : Relais werden NICHT geschaltet (nur Entscheidung loggen/publishen).
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

SHADOW = '--shadow' in sys.argv

# ═══════════════════════════════════════════════════════════════════════════
VERSION = "v2.0-Port"          # fox2db_logic.h v2.9-Port
MAX_LOG_BYTES = 100 * 1024

# ── CONFIG (1:1 aus fox2db_logic.h) ─────────────────────────────────────────
MIN_EXCESS       = 2500.0
MAX_GRID_DRAW    = 900.0
MAX_SOC          = 100.0
HYSTERESIS       = 505.0
STABILIZATION    = 2
EMERGENCY_MARGIN = 120.0                          # harter Abwurf erst bei G + Margin
EMERGENCY_IMPORT = MAX_GRID_DRAW + EMERGENCY_MARGIN   # = 1020.0 (an G gekoppelt)
BAT_DISCHARGE_TH = -110.0
SWEET_SPOT_PCC   = 160.0
MAX_DROP_RATE    = -20.0
LADESPERRE_HYST  = 0.15                           # Hysterese-Band für Ladesperre-Ratio (Latch)
BAT1_CHARGE_FACTOR = 0.5                           # Anteil der Sofar-Ladung (bat1>0), der als EBox-Überschuss zählt
DD_LOWER         = 6
DD_UPPER         = 8
DD_CHARGE_TARGET = 7
PCC_PEAK_TH      = 20000.0
PCC_HARD_TH      = 22000.0     # bedingungsloser DO4-Trigger

LADESPERRE_ENABLE = True       # YAML default
LADESPERRE_RATIO  = 0.5        # YAML default (sofar/ratio)

# STATE_TO_POWER {0:0,1:3000,2:3650,3:6650,4:3900,5:7100,6:7800,7:11400}
_STATE_POWER  = [0, 3000, 3650, 6650, 3900, 7100, 7800, 11400]
# SORTED_STATES nach Leistung sortiert: [0,1,2,4,3,5,6,7]
SORTED_STATES = [0, 1, 2, 4, 3, 5, 6, 7]

def state_power(s: int) -> int:
    return _STATE_POWER[s] if 0 <= s <= 7 else 0

def sorted_index(s: int) -> int:
    for i in range(8):
        if SORTED_STATES[i] == s:
            return i
    return 0

PATHS = {
    'log':         '/tmp/fox2dbEasy.log',
    'state':       '/tmp/fox2dbEasy_state.json',   # fox::State über Cron-Zyklen
    'relay_state': '/tmp/current_relay_state_easy.txt',  # Kompat (externe Leser)
    'result':      '/tmp/fox2dbEasy_result.txt',   # letzter State für Wrapper
}

MQTT_CFG = {
    'broker':      '192.168.178.218',
    'port':         1883,
    'topic':       'inverter/power_grid_exchange/json',
    'zaehl_topic': 'pv_zaehl2/#',
    'ebox_topic':  'ebox/pwr',
    'timeout':      43,
    'pub_topic':   'fox2db/easy/state',
}

TZ = zoneinfo.ZoneInfo("Europe/Berlin")

# ═══════════════════════════════════════════════════════════════════════════
#                         DC-KLARHIMMEL-FORECAST (NOAA, 1:1 aus Header)
# ═══════════════════════════════════════════════════════════════════════════

LAT, LON = 47.6811, 11.5732
# (tilt, Azimut-Süd, Nennleistung)
ARRAYS      = [(25, 80, 27854), (60, -5, 11138)]
ARRAYS_EAST = [(41, -74, 19852), (60, 90, 2078), (32, 94, 2378)]
_KT = [0, .331, .402, .563, .838, .909, .880, .840, .820, .760, .600, .350, .134]

def kt_month(m: int) -> float:
    return _KT[m] if 1 <= m <= 12 else 0.60

def _d2r(d: float) -> float:
    return d * math.pi / 180.0

# NOAA-Sonnenstand: Elevation + Azimut (von Nord, im Uhrzeigersinn) für unix-UTC.
def sun_pos(t_utc: float) -> Tuple[float, float]:
    g = time.gmtime(t_utc)
    hour  = g.tm_hour + g.tm_min / 60.0 + g.tm_sec / 3600.0
    yday0 = g.tm_yday - 1                 # C struct tm tm_yday ist 0-basiert
    gamma = 2.0 * math.pi / 365.0 * (yday0 + (hour - 12) / 24.0)
    eqtime = 229.18 * (0.000075 + 0.001868 * math.cos(gamma) - 0.032077 * math.sin(gamma)
                       - 0.014615 * math.cos(2 * gamma) - 0.040849 * math.sin(2 * gamma))
    decl = (0.006918 - 0.399912 * math.cos(gamma) + 0.070257 * math.sin(gamma)
            - 0.006758 * math.cos(2 * gamma) + 0.000907 * math.sin(2 * gamma)
            - 0.002697 * math.cos(3 * gamma) + 0.00148 * math.sin(3 * gamma))
    tst = hour * 60.0 + eqtime + 4.0 * LON           # tz = UTC
    ha  = tst / 4.0 - 180.0                            # Stundenwinkel (Grad)
    har, latr = _d2r(ha), _d2r(LAT)
    cosz = math.sin(latr) * math.sin(decl) + math.cos(latr) * math.cos(decl) * math.cos(har)
    cosz = max(-1.0, min(1.0, cosz))
    elev = 90.0 - math.acos(cosz) * 180.0 / math.pi
    az_s = math.atan2(math.sin(har), math.cos(har) * math.sin(latr) - math.tan(decl) * math.cos(latr))
    az   = (az_s * 180.0 / math.pi + 180.0 + 360.0) % 360.0   # 0 = N
    return elev, az

def cos_aoi(elev: float, azN: float, tilt: float, azS: float) -> float:
    e, b = _d2r(elev), _d2r(tilt)
    da = _d2r((azN - 180.0) - azS)
    return math.sin(e) * math.cos(b) + math.cos(e) * math.sin(b) * math.cos(da)

def calc_arrays(arrs, t_utc: float, month: int) -> float:
    elev, azN = sun_pos(t_utc)
    if elev <= 0:
        return 0.0
    am = min(1.0 / math.sin(_d2r(elev)), 37.0)
    T  = 0.7 ** (am ** 0.678)
    kt = kt_month(month)
    s = 0.0
    for tilt, azS, power in arrs:
        s += power * T * kt * max(0.0, cos_aoi(elev, azN, tilt, azS))
    return s

def dc_now(t_utc: float, month: int) -> float:
    return calc_arrays(ARRAYS, t_utc, month) + calc_arrays(ARRAYS_EAST, t_utc, month)

# ═══════════════════════════════════════════════════════════════════════════
#                         ZUSTAND (fox::State, persistent)
# ═══════════════════════════════════════════════════════════════════════════

class State:
    def __init__(self):
        self.relay_st    = 0
        self.stable      = 0
        self.last_excess = 0.0
        self.prot        = False          # Tiefentladeschutz (Hysterese)
        self.peak_today  = False          # pcc hat heute PCC_PEAK_TH überschritten
        self.ladesperre_latched = False   # Ladesperre-Latch (Hysterese gegen Flattern)
        self.last_yday   = -1
        self.pcc_buf     = [0.0] * 10
        self.pcc_n       = 0
        self.pcc_i       = 0

def load_state() -> State:
    st = State()
    try:
        d = json.loads(Path(PATHS['state']).read_text())
    except Exception:
        return st
    st.relay_st    = int(d.get('relay_st', 0))
    st.stable      = int(d.get('stable', 0))
    st.last_excess = float(d.get('last_excess', 0.0))
    st.prot        = bool(d.get('prot', False))
    st.peak_today  = bool(d.get('peak_today', False))
    st.ladesperre_latched = bool(d.get('ladesperre_latched', False))
    st.last_yday   = int(d.get('last_yday', -1))
    buf            = [float(x) for x in d.get('pcc_buf', [])]
    st.pcc_buf     = (buf + [0.0] * 10)[:10]
    st.pcc_n       = int(d.get('pcc_n', 0))
    st.pcc_i       = int(d.get('pcc_i', 0))
    return st

def save_state(st: State):
    Path(PATHS['state']).write_text(json.dumps({
        'relay_st': st.relay_st, 'stable': st.stable, 'last_excess': st.last_excess,
        'prot': st.prot, 'peak_today': st.peak_today, 'last_yday': st.last_yday,
        'ladesperre_latched': st.ladesperre_latched,
        'pcc_buf': st.pcc_buf, 'pcc_n': st.pcc_n, 'pcc_i': st.pcc_i,
    }))

class Inputs:
    def __init__(self, pcc, bat1, soc1, soc2, ebox_w):
        self.pcc, self.bat1, self.soc1, self.soc2, self.ebox_w = pcc, bat1, soc1, soc2, ebox_w

class Result:
    def __init__(self):
        self.final_state = 0
        self.changed     = False
        self.do4_pulse   = False
        self.ladesperre  = False
        self.excess = 0.0
        self.dc_expected = 0.0
        self.dc_delta = 0.0
        self.ratio = -1.0
        self.peak_h = -1
        self.win_end_h = -1
        self.trace = ""

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
            raw = msg.payload.decode()
            j   = json.loads(raw)
            pcc_val = j.get('ActivePower_PCC_Total')
            data = {
                'pcc':      float(pcc_val or 0) * 1000,
                'pcc_null': (pcc_val is None) or ('"ActivePower_PCC_Total":null' in raw),
                'bat1':     float(j.get('Power_Bat1') or 0) * 1000,
                'soc1':     float(j.get('SOC_Bat1')   or 0),
                'load_sys': float(j.get('ActivePower_Load_Sys') or 0) * 1000,
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


def fetch_ebox() -> Tuple[float, float]:
    """ebox/pwr (retained) → (soc2, ebox_w).  soc2=-1 = unbekannt (HOLD)."""
    got = {}

    def on_message(_c, _u, msg):
        try:
            j = json.loads(msg.payload.decode())
            got['soc']   = float(j.get('soc', -1))
            got['power'] = float(j.get('power_w', 0))
        except Exception:
            pass

    client = mqtt.Client(client_id="fox2dbeasy_ebox", clean_session=True)
    client.on_message = on_message
    try:
        client.connect(MQTT_CFG['broker'], MQTT_CFG['port'], keepalive=10)
        client.subscribe(MQTT_CFG['ebox_topic'], qos=0)   # retained → kommt sofort
        client.loop_start()
        deadline = time.monotonic() + 3.0
        while 'soc' not in got and time.monotonic() < deadline:
            time.sleep(0.05)
        client.loop_stop()
        client.disconnect()
    except Exception:
        pass
    return got.get('soc', -1.0), got.get('power', 0.0)

# ═══════════════════════════════════════════════════════════════════════════
#                         ENTSCHEIDUNGSLOGIK (1:1 aus fox2db_logic.h)
# ═══════════════════════════════════════════════════════════════════════════

def decide(in_: Inputs, relay_st: int, prot: bool) -> Tuple[int, str, float]:
    ebox_eff = max(in_.ebox_w, float(state_power(relay_st))) if relay_st > 0 else 0.0
    # Sofar-Entladung (bat1<0) voll; von der Sofar-Ladung (bat1>0) nur der Anteil BAT1_CHARGE_FACTOR
    bat1_eff = in_.bat1 if in_.bat1 < 0 else in_.bat1 * BAT1_CHARGE_FACTOR
    excess = in_.pcc + ebox_eff + bat1_eff

    if in_.soc2 < 0:
        ea = in_.ebox_w if relay_st > 0 else 0.0
        return relay_st, "EBOX_SOC_UNKNOWN_HOLD", in_.pcc + ea + bat1_eff

    if in_.pcc > PCC_PEAK_TH and in_.soc2 < MAX_SOC:
        next_st = min(relay_st + 1, 7)
        if next_st > relay_st:
            return next_st, f"PCC_OVER_20KW (SOC={in_.soc2:.0f}% State{relay_st}->{next_st})", excess

    if prot:
        if in_.soc2 < DD_CHARGE_TARGET:
            return 1, f"EMERGENCY_CHARGE_TO_7% ({in_.soc2:.1f}%)", excess
        return 0, f"CHARGE_TARGET_REACHED ({in_.soc2:.1f}%)", excess

    if excess < MIN_EXCESS:
        return 0, f"INSUFFICIENT_EXCESS ({excess:.0f}W)", excess
    budget = excess + MAX_GRID_DRAW
    best, best_pow = 0, -1
    for s in range(8):
        p = state_power(s)
        if p <= budget and p > best_pow:
            best, best_pow = s, p
    trace = f"POWER_MATCHING (Excess: {excess:.0f}W, Budget: {budget:.0f}W)"

    if best > relay_st:                       # Ramp-Limiting (State-Nr.-Vergleich)
        idx = sorted_index(relay_st)
        next_st = SORTED_STATES[idx + 1 if idx + 1 < 8 else 7]
        if best > next_st:
            trace += f" | RAMP_LIMITED ({best}->{next_st})"
            best = next_st

    return best, trace, excess


def apply_guards(best: int, soc2: float, ladesperre: bool, trace: str) -> Tuple[int, bool, str]:
    if ladesperre:
        if best != 0:
            trace += " | GUARD:LADESPERRE_BIS_PCC_20KW"
        return 0, True, trace
    if soc2 >= MAX_SOC:
        if best != 0:
            trace += " | GUARD:BATTERY_FULL_STOP"
        return 0, True, trace
    if 0 <= soc2 < DD_LOWER:
        if best != 1:
            trace += " | GUARD:CRITICAL_SOC_PROTECTION_ACTIVATE"
        return 1, True, trace
    return best, False, trace


def apply_blocking(best: int, relay_st: int, pcc: float, bat1: float,
                   stable: int, drop_rate: float, trace: str) -> Tuple[int, bool, str]:
    if best == relay_st:
        return relay_st, False, trace
    pwr_diff = abs(state_power(best) - state_power(relay_st))
    up = state_power(best) > state_power(relay_st)
    if pcc < -EMERGENCY_IMPORT and not up:
        return best, True, trace + f" | EMERGENCY_FORCE (Import={pcc:.0f}W)"
    if up and abs(pcc) < SWEET_SPOT_PCC:
        return relay_st, False, trace + " | SWEET_SPOT_HOLD"
    if up and drop_rate < MAX_DROP_RATE and drop_rate != 0:
        return relay_st, False, trace + " | TREND_BLOCK"
    if up and bat1 < BAT_DISCHARGE_TH:
        return relay_st, False, trace + " | BAT_GUARD_BLOCK"
    if not up and stable < STABILIZATION:
        return relay_st, False, trace + " | STABILIZING"
    if not up and pwr_diff < HYSTERESIS:
        return relay_st, False, trace + " | HYSTERESIS"
    return best, True, trace


def step(in_: Inputs, st: State, now_local: dt.datetime,
         ladesperre_enable: bool, ladesperre_ratio: float) -> Result:
    r = Result()
    now_utc       = now_local.timestamp()
    local_sec_day = now_local.hour * 3600 + now_local.minute * 60 + now_local.second
    month         = now_local.month
    local_hour    = now_local.hour
    local_yday    = now_local.timetuple().tm_yday

    if local_yday != st.last_yday:            # Mitternachts-Reset
        st.pcc_n = 0
        st.pcc_i = 0
        st.peak_today = False
        st.ladesperre_latched = False
        st.last_yday  = local_yday

    r.dc_expected = dc_now(now_utc, month)

    st.pcc_buf[st.pcc_i] = in_.pcc
    st.pcc_i = (st.pcc_i + 1) % 10
    if st.pcc_n < 10:
        st.pcc_n += 1
    pcc_avg_valid = st.pcc_n >= 3
    pcc_avg = sum(st.pcc_buf[:st.pcc_n]) / st.pcc_n if pcc_avg_valid else 0.0

    # Peak-Fenster immer berechnen (auch ohne ladesperre_enable) → für Reporting
    midnight = now_utc - local_sec_day
    best_w = 0.0
    peak_h_loc, win_end_loc = -1, -1
    for h in range(5, 21):
        w = dc_now(midnight + h * 3600, month)
        if w > best_w:
            best_w, peak_h_loc = w, h
        if w > PCC_PEAK_TH:
            win_end_loc = h
    r.peak_h    = peak_h_loc if best_w > PCC_PEAK_TH else -1
    r.win_end_h = win_end_loc

    if in_.pcc > PCC_PEAK_TH:
        st.peak_today = True

    # Ist-Wetter-Ratio (ratio > Schwelle ⇒ Schlechtwetter); -1 = nicht berechenbar
    if pcc_avg_valid and r.dc_expected > 5000:
        r.ratio = (r.dc_expected - (pcc_avg + in_.ebox_w + in_.bat1)) / r.dc_expected

    ladesperre = False
    if ladesperre_enable:
        has_peak  = best_w > PCC_PEAK_TH
        in_window = (has_peak and win_end_loc >= 0 and not st.peak_today
                     and local_hour <= peak_h_loc)
        # LATCH mit Hysterese gegen Flattern an der Ratio-Schwelle:
        if not in_window:
            st.ladesperre_latched = False
        elif r.ratio >= 0.0:
            if not st.ladesperre_latched and r.ratio <= ladesperre_ratio:
                st.ladesperre_latched = True          # LOCK: belegtes Gutwetter
            elif st.ladesperre_latched and r.ratio >= ladesperre_ratio + LADESPERRE_HYST:
                st.ladesperre_latched = False         # RELEASE: klar Schlechtwetter
        ladesperre = in_window and st.ladesperre_latched
    r.ladesperre = ladesperre

    best, trace, excess = decide(in_, st.relay_st, st.prot)
    best, guard_fired, trace = apply_guards(best, in_.soc2, ladesperre, trace)

    drop_rate = (excess - st.last_excess) / 30.0 if st.last_excess > 0 else 0.0
    st.last_excess = excess

    if guard_fired:                            # Schutz unbypassbar
        final_state = best
        changed = (best != st.relay_st)
    else:
        final_state, changed, trace = apply_blocking(
            best, st.relay_st, in_.pcc, in_.bat1, st.stable, drop_rate, trace)

    st.stable = 0 if changed else st.stable + 1

    if in_.soc2 >= 0:                          # Deep-Discharge-Hysterese
        if in_.soc2 < DD_LOWER:
            st.prot = True
        elif in_.soc2 >= DD_UPPER:
            st.prot = False

    need_down = (in_.pcc > PCC_HARD_TH) or \
                ((in_.pcc > PCC_PEAK_TH) and (not trace.startswith("PCC_OVER_20KW") or ladesperre))
    r.do4_pulse = need_down

    st.relay_st  = final_state
    r.final_state = final_state
    r.changed     = changed
    r.excess      = excess
    r.dc_delta    = r.dc_expected - (in_.pcc + in_.ebox_w + in_.bat1)
    r.trace       = trace[:159]
    return r

# ═══════════════════════════════════════════════════════════════════════════
#                         RELAY CONTROL / PUBLISH
# ═══════════════════════════════════════════════════════════════════════════

def set_relay(state: int, do4_pulse: bool, reason: str = ""):
    if SHADOW:
        _log(f"Shadow: Relay→{state} (do4={do4_pulse}) unterdrückt | {reason}")
        return
    # sofar/auto wird bewusst NICHT angefasst: auto bleibt default=1 (ESP regelt
    # selbst); Übernahme durch externen Controller nur per manuellem MQTT sofar/auto 0.
    payloads = {
        'waveshare/relay/1': f'{{"v":{1 if state & 1 else 0}}}',
        'waveshare/relay/2': f'{{"v":{1 if state & 2 else 0}}}',
        'waveshare/relay/3': f'{{"v":{1 if state & 4 else 0}}}',
    }
    client = mqtt.Client(client_id="fox2dbeasy_relay", clean_session=True)
    try:
        client.connect(MQTT_CFG['broker'], MQTT_CFG['port'], keepalive=10)
        for topic, payload in payloads.items():
            client.publish(topic, payload, qos=0).wait_for_publish(timeout=3)
        if do4_pulse:                          # CH4 3s-Puls → WR2 abregeln
            client.publish('waveshare/relay/4', '{"v":1}', qos=0).wait_for_publish(timeout=3)
            time.sleep(3)
            client.publish('waveshare/relay/4', '{"v":0}', qos=0).wait_for_publish(timeout=3)
    except Exception as e:
        _log(f"MQTT relay error: {e}")
    finally:
        try: client.disconnect()
        except Exception: pass
    _write(PATHS['relay_state'], state)
    _log(f"Relay: State→{state} (do4={do4_pulse}) | {reason}")


def publish_state(r: Result, in_: Inputs):
    payload = json.dumps({
        "version": VERSION, "state": r.final_state, "changed": r.changed,
        "pcc": round(in_.pcc), "bat1": round(in_.bat1),
        "soc1": round(in_.soc1, 1), "soc2": round(in_.soc2, 1),
        "ebox": round(in_.ebox_w), "excess": round(r.excess),
        "dc_expected": round(r.dc_expected), "dc_delta": round(r.dc_delta),
        "ratio": round(r.ratio, 2), "ladesperre": r.ladesperre, "do4": r.do4_pulse,
        "peak_h": r.peak_h, "win_end_h": r.win_end_h,
        "trace": r.trace,                      # Gründe — Debug-Feature
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
#                              MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    _log("--- Start Cycle ---")

    mqtt_data = fetch_mqtt()
    if mqtt_data is None:
        # Frische-Check fehlgeschlagen → Sicher auf State 0 (wie YAML MQTT_STALE_SAFE)
        _log("SHUTDOWN: MQTT failed → MQTT_STALE_SAFE")
        st = load_state()
        st.relay_st = 0
        save_state(st)
        if not SHADOW:
            set_relay(0, False, "MQTT_STALE_SAFE")
        _write(PATHS['result'], '0')
        return

    z2       = fetch_z2()
    soc2, ebox_w = fetch_ebox()

    pcc  = mqtt_data['pcc']
    bat1 = mqtt_data['bat1']
    soc1 = mqtt_data['soc1']
    # PCC: Inverter-Wert, Z2 nur als Fallback bei null-Lesefehler (wie YAML)
    if mqtt_data['pcc_null'] and z2 != 0.0:
        pcc = -z2
        _log(f"PCC=null → Z2-Fallback PCC={pcc:.0f}W")

    st = load_state()
    in_ = Inputs(pcc=pcc, bat1=bat1, soc1=soc1, soc2=soc2, ebox_w=ebox_w)

    _log(f"Data: PCC={pcc:.0f}W  Bat1={bat1:.0f}W  SOC1={soc1:.1f}%  SOC2={soc2:.1f}%  "
         f"EBox={ebox_w:.0f}W  State={st.relay_st}  Stable={st.stable}  Prot={st.prot}")

    now_local = dt.datetime.now(TZ)
    r = step(in_, st, now_local, LADESPERRE_ENABLE, LADESPERRE_RATIO)

    # Zustand IMMER fortschreiben (auch im Shadow) — sonst evolviert die FSM nicht.
    save_state(st)

    _log(f"Result: State {r.final_state} (changed={r.changed} ladesperre={r.ladesperre} "
         f"do4={r.do4_pulse} ratio={r.ratio:.2f}) TRACE: {r.trace}")

    _write(PATHS['result'], r.final_state)
    set_relay(r.final_state, r.do4_pulse, r.trace)
    publish_state(r, in_)


if __name__ == '__main__':
    main()
