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
PCC_AVG_N        = 3           # Ueberschuss auf 3-Min-Mittel (60-s-Zyklus)
RAMP_FREE_FACTOR = 1.3         # Ueberschuss >= Zielstufe * Faktor -> Rampe entfaellt
EMERGENCY_MARGIN = 120.0                          # harter Abwurf erst bei G + Margin
EMERGENCY_IMPORT = MAX_GRID_DRAW + EMERGENCY_MARGIN   # = 1020.0 (an G gekoppelt)
BAT_DISCHARGE_TH = -110.0
SWEET_SPOT_PCC   = 160.0
MAX_DROP_RATE    = -20.0
LADESPERRE_HYST  = 0.25                           # Hysterese-Band für Ladesperre-Ratio (Latch) — identisch zu fox2db_logic.h
BAT1_CHARGE_FACTOR = 0.5                           # Anteil der Sofar-Ladung (bat1>0), der als EBox-Überschuss zählt
DD_LOWER         = 6
DD_UPPER         = 8
DD_CHARGE_TARGET = 7
PCC_PEAK_TH      = 20000.0
PCC_HARD_TH      = 22000.0     # (nicht mehr im DO4-Pfad, s. need_down)
DO4_EBOX_FULL_W  = 11000.0     # ab hier laedt die EBox wirklich voll
# SOC1-Tor: Der Sofar-Hausakku hat Vorrang. Solange er heute nicht einmal
# ueber SOC1_FULL_TH stand, darf die EBox hoechstens SOC1_GATE_MAX_W ziehen —
# erst danach sind die grossen Stufen frei. Tages-Latch wie peak_today: einmal
# gesehen genuegt, ein spaeterer Abfall schliesst das Tor nicht wieder.
# Der Deckel ist dabei kein fester Wert mehr, sondern das Maximum aus
# SOC1_GATE_MAX_W und dem AKKU-NEUTRALEN Budget (pcc + ebox_eff): so viel kann
# die EBox ziehen, ohne dem Akku ein Watt wegzunehmen. Grund: der Sofar-Akku
# laedt hart mit hoechstens 2,5 kW (gemessen: MAX(Power_Bat1) = 2.5 kW ueber
# alle Tage). Wer 14 kW einspeist, kann die EBox voll laufen lassen und der
# Akku bekommt trotzdem seine volle Ladeleistung — der Vorrang ist dann
# gegenstandslos, der alte Fixdeckel hat nur Einspeisung verschenkt.
# Warum pcc + ebox_eff und nicht der Ueberschuss: die Summe ist gegen den
# eigenen Hochlauf invariant. Zieht die EBox mehr, faellt pcc um genau denselben
# Betrag, um den ebox_eff steigt — der Deckel bleibt stehen, statt zuzuschnappen,
# sobald die Einspeisung aufgezehrt ist. Ein Latch gegen Flattern braucht es
# deshalb nicht. SOC1_GATE_MAX_W bleibt als Untergrenze stehen, damit die
# Freigabe den Deckel nur lockern und nie verschaerfen kann.
SOC1_FULL_TH     = 99.0        # "einmal >99 %" gesehen
SOC1_GATE_MAX_W  = 5000.0      # Mindestdeckel bis dahin
LADESPERRE_MONTH_FROM = 5      # Mai; ausserhalb Mai-August keine
LADESPERRE_MONTH_TO   = 8      # August; Ladesperre, nur DO4 >20 kW

LADESPERRE_ENABLE = True       # YAML default
LADESPERRE_RATIO  = 0.5        # YAML default (sofar/ratio)
LADESPERRE_NOW_RATIO = 0.20    # Momentan-Ratio, ab der Bewölkung als belegt gilt

# STATE_TO_POWER {0:0,1:3000,2:3650,3:6650,4:3900,5:7100,6:7800,7:11400}
_STATE_POWER  = [0, 3000, 3650, 6650, 3900, 7100, 7800, 11400]
# SORTED_STATES nach Leistung sortiert, OHNE State 2 (ch2 nur in Kombination erlaubt): [0,1,4,3,5,6,7]
SORTED_STATES = [0, 1, 4, 3, 5, 6, 7]

def state_power(s: int) -> int:
    return _STATE_POWER[s] if 0 <= s <= 7 else 0

def sorted_index(s: int) -> int:
    for i in range(len(SORTED_STATES)):
        if SORTED_STATES[i] == s:
            return i
    return 0

def state_capped(s: int, max_w: float) -> int:
    """Hoechste Stufe, die den Leistungsdeckel nicht reisst.

    Gesucht wird in SORTED_STATES, damit State 2 auch hier aussen vor bleibt
    (wie in decide()).
    """
    if state_power(s) <= max_w:
        return s
    best, best_pow = 0, -1
    for st in SORTED_STATES:
        p = state_power(st)
        if p <= max_w and p > best_pow:
            best, best_pow = st, p
    return best

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
# Feldparameter je String gefittet (gen_pv_strings.py, 14 klare Tage):
# Sofar  PV1 West / PV2 Sued,  FoxESS pv2 Ost / pv1 West.
ARRAYS      = [(60, 33, 19430), (68, -12, 7690)]
ARRAYS_EAST = [(59, -29, 22036), (67, 32, 2781)]
# Standorthorizont: hoher Baumbestand im Ostsektor. Unter der Baumlinie bleibt
# nur Diffusstrahlung. Der Ertrag setzt dadurch ganzjaehrig rund zwei Stunden
# nach Sonnenaufgang ein (Messung: Kante bei 22-26 Grad, s. clearsky.tex).
HOR_AZ_SPLIT = 120.0     # Grenze Ost-/Suedsektor (Azimut von Nord)
HOR_EAST     = 23.0      # Elevation, unter der Ost verschattet ist
HOR_SOUTH    = 15.0      # dito Suedost
HOR_DIFFUSE  = 0.25      # Restanteil im Schatten
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
    # Horizont: steht die Sonne unter der Baumlinie, nur Diffusanteil.
    hor = HOR_EAST if azN < HOR_AZ_SPLIT else HOR_SOUTH
    shade = 1.0 if elev >= hor else HOR_DIFFUSE
    s = 0.0
    for tilt, azS, power in arrs:
        s += power * T * kt * max(0.0, cos_aoi(elev, azN, tilt, azS))
    return s * shade

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
        self.badweather_today = False     # Momentanleistung lag heute >= LADESPERRE_NOW_RATIO unter Klarhimmel
        self.soc1_full_today = False      # SOC1 stand heute einmal ueber SOC1_FULL_TH -> Deckel faellt
        self.last_yday   = -1
        self.pcc3_buf    = [0.0] * PCC_AVG_N   # PCC-Ringpuffer fuer das 3-Min-Mittel
        self.pcc3_n      = 0
        self.pcc3_i      = 0
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
    st.badweather_today = bool(d.get('badweather_today', False))
    st.soc1_full_today = bool(d.get('soc1_full_today', False))
    st.last_yday   = int(d.get('last_yday', -1))
    buf            = [float(x) for x in d.get('pcc_buf', [])]
    st.pcc_buf     = (buf + [0.0] * 10)[:10]
    st.pcc_n       = int(d.get('pcc_n', 0))
    st.pcc_i       = int(d.get('pcc_i', 0))
    buf3           = [float(x) for x in d.get('pcc3_buf', [])]
    st.pcc3_buf    = (buf3 + [0.0] * PCC_AVG_N)[:PCC_AVG_N]
    st.pcc3_n      = int(d.get('pcc3_n', 0))
    st.pcc3_i      = int(d.get('pcc3_i', 0))
    return st

def save_state(st: State):
    Path(PATHS['state']).write_text(json.dumps({
        'relay_st': st.relay_st, 'stable': st.stable, 'last_excess': st.last_excess,
        'prot': st.prot, 'peak_today': st.peak_today, 'last_yday': st.last_yday,
        'ladesperre_latched': st.ladesperre_latched,
        'badweather_today': st.badweather_today,
        'soc1_full_today': st.soc1_full_today,
        'pcc_buf': st.pcc_buf, 'pcc_n': st.pcc_n, 'pcc_i': st.pcc_i,
        'pcc3_buf': st.pcc3_buf, 'pcc3_n': st.pcc3_n, 'pcc3_i': st.pcc3_i,
    }))

class Inputs:
    def __init__(self, pcc, bat1, soc1, soc2, ebox_w, pcc_valid=True):
        self.pcc, self.bat1, self.soc1, self.soc2, self.ebox_w = pcc, bat1, soc1, soc2, ebox_w
        self.pcc_valid = pcc_valid            # False = PCC kam als null (Lesefehler)

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
        self.ratio_now = -1.0
        self.peak_h = -1
        self.win_end_h = -1
        self.soc1_gate = False   # True = SOC1-Deckel aktiv (SOC1 heute noch nie >99 %)
        self.soc1_gate_w = 0.0   # wirksamer Deckel dieses Zyklus (Reporting)
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

# pcc_excess = normalerweise der gueltige Minutenwert der Sofar (in_.pcc). Nur
# wenn der PCC als null hereinkommt (Lesefehler, in_.pcc_valid == False), tritt
# das 3-Min-Mittel der letzten gueltigen Werte an seine Stelle — statt der 0.
# Gemittelt wird also nicht geglaettet: ein echter Einbruch schlaegt weiter
# sofort durch, sonst wird waehrend einer Wolke aus dem Netz geladen.
def decide(in_: Inputs, relay_st: int, prot: bool,
           pcc_excess: float) -> Tuple[int, str, float]:
    ebox_eff = max(in_.ebox_w, float(state_power(relay_st))) if relay_st > 0 else 0.0
    # Sofar-Entladung (bat1<0) voll; von der Sofar-Ladung (bat1>0) nur der Anteil BAT1_CHARGE_FACTOR
    bat1_eff = in_.bat1 if in_.bat1 < 0 else in_.bat1 * BAT1_CHARGE_FACTOR
    excess = pcc_excess + ebox_eff + bat1_eff

    if in_.soc2 < 0:
        ea = in_.ebox_w if relay_st > 0 else 0.0
        return relay_st, "EBOX_SOC_UNKNOWN_HOLD", pcc_excess + ea + bat1_eff

    # DO4-Gefahr: sofort auf Stufe 7. Stufenweises Hochrampen kostet Minuten, in
    # denen die Einspeisung ueber 20 kW bleibt und DO4 den WR2 abwirft — die volle
    # Aufnahme ist immer billiger als die Abregelung.
    if in_.pcc > PCC_PEAK_TH and in_.soc2 < MAX_SOC:
        next_st = 7
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
        if s == 2:
            continue
        p = state_power(s)
        if p <= budget and p > best_pow:
            best, best_pow = s, p
    trace = f"POWER_MATCHING (Excess: {excess:.0f}W, Budget: {budget:.0f}W)"

    # Rampe nur, wenn der Ueberschuss die Zielstufe nicht klar traegt. Sie schuetzt
    # vor Netzbezug bei knappem Ueberschuss — bei 19 kW Sonne und 11,4 kW Zielstufe
    # gibt es nichts abzutasten, dann kostet jeder Zwischenschritt nur Ertrag.
    if best > relay_st and excess >= state_power(best) * RAMP_FREE_FACTOR:
        trace += f" | RAMP_FREE ({excess:.0f}W traegt State{best})"
    elif best > relay_st:                     # Ramp-Limiting (State-Nr.-Vergleich)
        idx = sorted_index(relay_st)
        next_st = SORTED_STATES[idx + 1 if idx + 1 < len(SORTED_STATES) else len(SORTED_STATES) - 1]
        if best > next_st:
            trace += f" | RAMP_LIMITED ({best}->{next_st})"
            best = next_st

    return best, trace, excess


def apply_guards(best: int, soc2: float, pcc: float, ladesperre: bool,
                 soc1_gate: bool, soc1_gate_max_w: float,
                 trace: str) -> Tuple[int, bool, str]:
    # Reihenfolge: volle Batterie schlaegt alles (sie kann nichts mehr aufnehmen,
    # dann muss DO4 ran). Danach die DO4-Gefahr — sie schlaegt Ladesperre und
    # Tiefentladeschutz, weil beide durch Laden erfuellt statt verletzt werden,
    # und sie muss am Ramp-/Stabilize-Blocking vorbei: genau das hat bisher den
    # Hochlauf verzoegert (Trace "PCC_OVER_20KW ... | STABILIZING").
    if soc2 >= MAX_SOC:
        if best != 0:
            trace += " | GUARD:BATTERY_FULL_STOP"
        return 0, True, trace
    if soc2 >= 0 and pcc > PCC_PEAK_TH:
        if best != 7:
            trace += " | GUARD:DO4_RISK_FORCE_STATE7"
        return 7, True, trace
    if ladesperre:
        if best != 0:
            trace += " | GUARD:LADESPERRE_BIS_PCC_20KW"
        return 0, True, trace
    if 0 <= soc2 < DD_LOWER:
        if best != 1:
            trace += " | GUARD:CRITICAL_SOC_PROTECTION_ACTIVATE"
        return 1, True, trace
    # SOC1-Tor: Der Sofar-Akku hat Vorrang — mehr als SOC1_GATE_MAX_W erst, wenn
    # er heute einmal ueber SOC1_FULL_TH stand. Anders als die vier Regeln darueber
    # waehlt das Tor keine Stufe aus, es begrenzt nur die schon gewaehlte; deshalb
    # steht es am Ende und setzt guard_fired NICHT. Zwei Folgen, beide gewollt:
    # die vorherigen Guards kehren vorher zurueck, ihr erzwungener State bleibt
    # also unangetastet (bei pcc > 20 kW gewinnt Stufe 7 und DO4 bleibt intakt),
    # und apply_blocking() laeuft weiter — Rampe, Sweet-Spot- und Bat-Guard-Schutz
    # gelten auch fuer den gedeckelten Hochlauf.
    if soc1_gate:
        capped = state_capped(best, soc1_gate_max_w)
        if capped != best:
            trace += f" | SOC1_GATE (max {soc1_gate_max_w:.0f}W) State{best}->{capped}"
            best = capped
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
        st.pcc3_n = 0
        st.pcc3_i = 0
        st.peak_today = False
        st.ladesperre_latched = False
        st.badweather_today = False
        st.soc1_full_today = False             # SOC1-Tor faellt taeglich neu zu
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

    # SOC1-Tor: einmal ueber der Schwelle gesehen reicht fuer den ganzen Tag.
    # Ein fehlender MQTT-Wert kommt als 0 herein und kann das Tor darum nie
    # versehentlich oeffnen — bei stummem Broker bleibt der Deckel liegen, was
    # die sichere Richtung ist.
    if in_.soc1 > SOC1_FULL_TH:
        st.soc1_full_today = True
    r.soc1_gate = not st.soc1_full_today

    # Ist-Wetter-Ratio (ratio > Schwelle ⇒ Schlechtwetter); -1 = nicht berechenbar
    if pcc_avg_valid and r.dc_expected > 5000:
        r.ratio = (r.dc_expected - (pcc_avg + in_.ebox_w + in_.bat1)) / r.dc_expected
    # Momentan-Ratio aus den UNGEMITTELTEN Werten: pcc_avg verschleift Wolken-
    # lücken zu Sonnenschein, der Rohwert zeigt die Wolke sofort. Nur mit
    # gültigem PCC — ein null würde als volle Verschattung missdeutet.
    if r.dc_expected > 5000 and in_.pcc_valid:
        r.ratio_now = (r.dc_expected - (in_.pcc + in_.ebox_w + in_.bat1)) / r.dc_expected

    ladesperre = False
    if ladesperre_enable:
        has_peak  = best_w > PCC_PEAK_TH
        # Harte Monatsschranke: ausserhalb Mai-August nur DO4-Abregelung (>20 kW),
        # der Akku laedt sofort. Nicht dem impliziten has_peak ueberlassen.
        in_season = LADESPERRE_MONTH_FROM <= now_local.month <= LADESPERRE_MONTH_TO
        in_window = (in_season and has_peak and win_end_loc >= 0 and not st.peak_today
                     and local_hour <= peak_h_loc)
        # Momentan-Wolkenerkennung: liegt die AKTUELLE Leistung >= 20 % unter dem
        # Klarhimmel-Modell, ist das Gutwetter für heute widerlegt -> sofort laden.
        # Tages-Latch wie peak_today, sonst schnappt die Sperre bei der nächsten
        # Wolkenlücke wieder zu und die Ladung flattert im Minutentakt.
        if in_window and r.ratio_now >= LADESPERRE_NOW_RATIO:
            st.badweather_today = True
        # LATCH mit Hysterese gegen Flattern an der Ratio-Schwelle:
        if not in_window:
            st.ladesperre_latched = False
        elif r.ratio >= 0.0:
            if not st.ladesperre_latched and r.ratio <= ladesperre_ratio:
                st.ladesperre_latched = True          # LOCK: belegtes Gutwetter
            elif st.ladesperre_latched and r.ratio >= ladesperre_ratio + LADESPERRE_HYST:
                st.ladesperre_latched = False         # RELEASE: klar Schlechtwetter
        ladesperre = in_window and st.ladesperre_latched and not st.badweather_today
    r.ladesperre = ladesperre

    # Ringpuffer nur mit GUELTIGEN PCC-Werten fuellen — ein null darf den
    # Ersatzwert nicht mit einer 0 verwaessern.
    pcc_use = in_.pcc
    if in_.pcc_valid:
        st.pcc3_buf[st.pcc3_i] = in_.pcc
        st.pcc3_i = (st.pcc3_i + 1) % PCC_AVG_N
        if st.pcc3_n < PCC_AVG_N:
            st.pcc3_n += 1
    elif st.pcc3_n > 0:                        # null → Mittel der letzten gueltigen
        pcc_use = sum(st.pcc3_buf[:st.pcc3_n]) / st.pcc3_n

    # Akku-neutrales Budget: Einspeisung + was die EBox schon zieht. Bis hierhin
    # kann sie hochlaufen, ohne dem Sofar-Akku Ladeleistung zu entziehen.
    # ebox_eff wortgleich zu decide(), damit beide Seiten dieselbe Größe meinen.
    ebox_eff_gate = max(in_.ebox_w, float(state_power(st.relay_st))) if st.relay_st > 0 else 0.0
    r.soc1_gate_w = max(SOC1_GATE_MAX_W, max(0.0, pcc_use) + ebox_eff_gate)

    best, trace, excess = decide(in_, st.relay_st, st.prot, pcc_use)
    if not in_.pcc_valid:                      # im Trace sichtbar machen
        trace += f" | PCC_NULL_AVG3 ({pcc_use:.0f}W)"
    best, guard_fired, trace = apply_guards(best, in_.soc2, in_.pcc, ladesperre,
                                            r.soc1_gate, r.soc1_gate_w, trace)

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

    # DO4 haengt an der GEMESSENEN Aufnahme, nicht mehr am Trace-Text und nicht an
    # der 22-kW-Schwelle: abgeregelt wird nur, wenn die EBox nachweislich nichts
    # mehr aufnehmen kann. Solange die Laderampe die volle Last noch nicht erreicht
    # hat, waere der Puls verfrueht — die Aufnahme ist im Anmarsch, und WR2
    # abzuwerfen kostet echten Ertrag.
    #   ebox_w >= DO4_EBOX_FULL_W -> Stufe 7 steht wirklich an, mehr geht nicht
    #   soc2 >= MAX_SOC           -> Speicher voll, die EBox kann grundsaetzlich nicht
    # Alles dazwischen (Rampe laeuft, Relais gerade zu, Messung noch niedrig) haelt
    # DO4 zurueck.
    ebox_at_max = in_.ebox_w >= DO4_EBOX_FULL_W
    ebox_dead   = in_.soc2 >= MAX_SOC
    need_down   = (in_.pcc > PCC_PEAK_TH) and (ebox_at_max or ebox_dead)
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


def publish_state(r: Result, in_: Inputs, st: State):
    payload = json.dumps({
        "version": VERSION, "state": r.final_state, "changed": r.changed,
        "pcc": round(in_.pcc), "bat1": round(in_.bat1),
        "soc1": round(in_.soc1, 1), "soc2": round(in_.soc2, 1),
        "ebox": round(in_.ebox_w), "excess": round(r.excess),
        "dc_expected": round(r.dc_expected), "dc_delta": round(r.dc_delta),
        "ratio": round(r.ratio, 2), "ratio_now": round(r.ratio_now, 2),
        "badwx": st.badweather_today, "soc1_gate": r.soc1_gate,
        "soc1_gate_w": round(r.soc1_gate_w),
        "ladesperre": r.ladesperre, "do4": r.do4_pulse,
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
    in_ = Inputs(pcc=pcc, bat1=bat1, soc1=soc1, soc2=soc2, ebox_w=ebox_w,
                 pcc_valid=not mqtt_data['pcc_null'] or z2 != 0.0)

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
    publish_state(r, in_, st)


if __name__ == '__main__':
    main()
