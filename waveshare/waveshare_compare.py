#!/usr/bin/env python3
"""
waveshare_compare.py — Vergleich + DB-Sammler: fox2dbEasy (Pi) vs Waveshare ESP32.

Subscriptions:
  fox2db/easy/state   → easy-Entscheidung (version v2.0-Port)
  sofar/state         → waveshare-Entscheidung (volle Daten inkl. trace)
  soyo/calc           → Soyo-Sollwert (W/soc2/stale) → version 'soyo'

- Schreibt JEDE Entscheidung in MariaDB pv_decision_log
  (version='easy' / 'waveshare' / 'soyo') in derselben Tabelle.
- Soyo: bestimmt aus den letzten waveshare-Inputs den FW-Zweig (1:1 wie
  sofar_waveshare.yaml) → SOYO_*-Token (siehe dim_decisions kategorie='Soyo').
- Meldet zusätzlich Abweichungen (easy.state != waveshare.state) ins /tmp-Log.
Läuft endlos bis SIGTERM/SIGINT.
"""
import json, time, datetime, signal, sys, re, os
import paho.mqtt.client as mqtt
import pymysql

LOG    = '/tmp/waveshare_compare.log'
BROKER = '192.168.178.218'

DB_CFG = {'host': '192.168.178.218', 'db': 'wagodb', 'user': 'gh', 'pw': 'a12345'}

# letzter bekannter Zustand je Engine (für state_from + Divergenz-Vergleich)
last = {
    'easy':      {'state': None, 'trace': ''},
    'waveshare': {'state': None, 'trace': ''},
}
total    = 0
diverge  = 0
db_ok    = 0
db_err   = 0
do4_cnt  = 0         # gezählte ESP-DO4-Pulse seit Start
latest_auto = None   # ESP auto-Flag (1=ESP regelt selbst, 0=externer Controller) aus sofar/state
start_ts = time.monotonic()

DO4_PULSE_S = 3      # ESP feuert DO4 als 3s-Puls (sofar_waveshare.yaml: do4_script)

# letzte waveshare-Inputs (state/pcc/bat1/ebox/soc2) — für Soyo-Zweig-Bestimmung,
# da soyo/calc nur {W, soc2, stale} liefert.
last_ws_inputs = {}

_conn = None

def _i(v):
    try:    return int(round(float(v)))
    except Exception: return None

def log(msg):
    ts   = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG, 'a') as f:
        f.write(line + '\n')

# ── DB ───────────────────────────────────────────────────────────────────────
def db_connect():
    global _conn
    _conn = pymysql.connect(host=DB_CFG['host'], database=DB_CFG['db'],
                            user=DB_CFG['user'], password=DB_CFG['pw'],
                            connect_timeout=4, autocommit=True)

def parse_decision(trace: str) -> str:
    """Erstes Trace-Token als decision (wie prod): vor ' (' oder ' | ', ohne GUARD:."""
    if not trace:
        return 'UNKNOWN'
    tok = re.split(r'\s\(|\s\|\s', trace.strip())[0].strip()
    if tok.startswith('GUARD:'):
        tok = tok[6:]
    return tok[:64]

def write_decision(version: str, j: dict, auto):
    """Schreibt eine Engine-Entscheidung in pv_decision_log (inkl. auto-Flag)."""
    global _conn, db_ok, db_err
    state_to   = int(j.get('state', -1))
    state_from = last[version]['state']
    if state_from is None:
        state_from = state_to
    trace = j.get('trace', '')

    def _i(v):
        try:    return int(round(float(v)))
        except Exception: return None

    row = (
        datetime.datetime.now(),
        version,
        None if auto is None else int(auto),
        state_from, state_to,
        parse_decision(trace), trace[:255],
        _i(j.get('pcc')), _i(j.get('bat1')),
        round(float(j.get('soc2', 0) or 0), 1),
        _i(j.get('excess')), _i(j.get('ebox')),
        _i(j.get('dc_expected')), _i(j.get('dc_delta')),
    )
    sql = ("INSERT INTO pv_decision_log "
           "(ts,version,auto,state_from,state_to,decision,detail,"
           " pcc_w,bat1_w,soc,excess_w,ebox_w,dc_expected_w,dc_delta_w) "
           "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
    for attempt in (1, 2):
        try:
            if _conn is None:
                db_connect()
            with _conn.cursor() as cur:
                cur.execute(sql, row)
            db_ok += 1
            break
        except Exception as e:
            _conn = None
            if attempt == 2:
                db_err += 1
                log(f"DB-ERROR ({version}): {e}")

def write_do4_event(j: dict):
    """Persistiert einen ESP-DO4-Puls (do4==1 im sofar/state) nach pv_relay_events.
    controller = ESP-IP, damit erkennbar ist, welcher Controller ausgelöst hat
    (prod schreibt seine eigenen Zeilen mit controller='prod')."""
    global _conn, db_ok, db_err, do4_cnt

    def _i(v):
        try:    return int(round(float(v)))
        except Exception: return None

    controller = str(j.get('ip', 'esp'))[:24]
    pcc        = _i(j.get('pcc'))
    decision   = parse_decision(j.get('trace', ''))
    reason     = f"PCC>20kW pcc={pcc}W {decision}"[:255]
    row = (
        datetime.datetime.now(),
        'pulse', 'do4', controller,
        DO4_PULSE_S, _i(j.get('state')), reason,
    )
    sql = ("INSERT INTO pv_relay_events "
           "(ts,action,relay,controller,duration_s,state_new,reason) "
           "VALUES (%s,%s,%s,%s,%s,%s,%s)")
    for attempt in (1, 2):
        try:
            if _conn is None:
                db_connect()
            with _conn.cursor() as cur:
                cur.execute(sql, row)
            db_ok   += 1
            do4_cnt += 1
            log(f"DO4-PULS #{do4_cnt} controller={controller} ({reason})")
            break
        except Exception as e:
            _conn = None
            if attempt == 2:
                db_err += 1
                log(f"DB-ERROR (do4): {e}")

# ── Soyo ───────────────────────────────────────────────────────────────────────
def soyo_decision(stale, state, soc2, ebox, pcc):
    """1:1 die FW-Zweige aus sofar_waveshare.yaml (Soyo-Kalkulation, 60s)."""
    if stale:                                          return 'SOYO_STALE_OFF'
    if state != 0:                                     return 'SOYO_EBOX_ACTIVE_OFF'
    if soc2 is not None and 0.0 <= soc2 < 9.0:         return 'SOYO_DISCHARGE_PROTECT'
    if ebox is not None and ebox > 200:                return 'SOYO_BAT2_CHARGING_OFF'
    if pcc  is not None and pcc  > 200:                return 'SOYO_PV_SURPLUS_OFF'
    if pcc  is not None and pcc  < -100:               return 'SOYO_FEED_IN'
    return 'SOYO_BALANCED_BASELINE'

def handle_soyo(j):
    """Schreibt eine Soyo-Entscheidung (version='soyo') nach pv_decision_log.
    soyo/calc liefert nur {W, soc2, stale}; pcc/ebox/state aus last_ws_inputs."""
    global _conn, db_ok, db_err
    w     = _i(j.get('W')) or 0
    stale = bool(_i(j.get('stale')) or 0)
    soc2  = j.get('soc2')
    try:    soc2 = float(soc2)
    except Exception: soc2 = None

    inp   = last_ws_inputs
    if soc2 is None:
        soc2 = inp.get('soc2')
    state = inp.get('state', 0) or 0
    pcc   = inp.get('pcc')
    ebox  = inp.get('ebox')
    bat1  = inp.get('bat1')

    # Ohne ESP-Inputs lässt sich nur der stale-Zweig sicher bestimmen.
    if not stale and not inp:
        return

    dec    = soyo_decision(stale, state, soc2, ebox, pcc)
    s2_str = f"{soc2:.1f}" if soc2 is not None else "?"
    detail = (f"{dec} (W={w} pcc={pcc} ebox={ebox} "
              f"soc2={s2_str} stale={int(stale)})")[:255]
    row = (
        datetime.datetime.now(), 'soyo', latest_auto,
        int(state), int(state), dec, detail,
        pcc, bat1, round(float(soc2), 1) if soc2 is not None else None,
        w, ebox, None, None,          # excess_w-Spalte trägt den Soyo-Sollwert W
    )
    sql = ("INSERT INTO pv_decision_log "
           "(ts,version,auto,state_from,state_to,decision,detail,"
           " pcc_w,bat1_w,soc,excess_w,ebox_w,dc_expected_w,dc_delta_w) "
           "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
    for attempt in (1, 2):
        try:
            if _conn is None:
                db_connect()
            with _conn.cursor() as cur:
                cur.execute(sql, row)
            db_ok += 1
            break
        except Exception as e:
            _conn = None
            if attempt == 2:
                db_err += 1
                log(f"DB-ERROR (soyo): {e}")

# ── Auswertung ────────────────────────────────────────────────────────────────
def summary():
    elapsed = int(time.monotonic() - start_ts)
    pct = f"{diverge/total*100:.1f}%" if total else "n/a"
    log(f"SUMMARY: {total} Vergleiche, {diverge} Abweichungen ({pct}) "
        f"nach {elapsed//3600}h{elapsed%3600//60}m | DB ok={db_ok} err={db_err} "
        f"| DO4-Pulse={do4_cnt}")

MODEL_CACHE = '/tmp/sofar_model_last.json'

def _cache_model(j):
    """Legt die Modellwerte des ESP (peak_h, win_end_h, dc_expected) als Datei ab.

    sofar/state ist NICHT retained und wird nur 1x/60s gesendet — ein Minutencron
    wie r290_boost trifft die Message darum meistens nicht. Dieser Daemon haengt
    dauerhaft am Topic und sieht jede; die Datei ist der Umweg dorthin.
    """
    try:
        payload = {
            'ts':          time.time(),
            'peak_h':      j.get('peak_h'),
            'win_end_h':   j.get('win_end_h'),
            'dc_expected': j.get('dc_expected'),
            'ratio_now':   j.get('ratio_now'),
            'ratio_ist':   j.get('ratio_ist'),
            'badwx':       j.get('badwx'),
        }
        tmp = MODEL_CACHE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(payload, f)
        os.replace(tmp, MODEL_CACHE)
    except Exception:
        pass


def on_message(client, userdata, msg):
    global total, diverge, latest_auto
    try:
        j = json.loads(msg.payload.decode())
    except Exception:
        return

    if msg.topic == 'soyo/calc':
        handle_soyo(j)
        return

    if msg.topic == 'fox2db/easy/state':
        version = 'easy'
    elif msg.topic == 'sofar/state':
        version = 'waveshare'
        _cache_model(j)
    else:
        return

    # auto-Flag: waveshare liefert es direkt; easy bekommt den zuletzt bekannten Wert.
    if version == 'waveshare' and j.get('auto') is not None:
        latest_auto = int(j.get('auto'))
    auto = latest_auto

    # waveshare-Inputs für die Soyo-Zweig-Bestimmung mitführen
    if version == 'waveshare':
        try:    s2 = float(j.get('soc2'))
        except Exception: s2 = None
        last_ws_inputs.update({
            'state': _i(j.get('state')) or 0,
            'pcc':   _i(j.get('pcc')),
            'bat1':  _i(j.get('bat1')),
            'ebox':  _i(j.get('ebox')),
            'soc2':  s2,
        })

    # DB-Schreiben (jede Entscheidung, beide Engines) inkl. auto-Flag
    write_decision(version, j, auto)

    # ESP-DO4-Puls separat als Relais-Event protokollieren (mit controller-ID)
    if version == 'waveshare' and j.get('do4'):
        write_do4_event(j)

    state = int(j.get('state', -1))
    trace = j.get('trace', '')

    # Divergenz-Vergleich am Waveshare-Takt (authoritativ, 60s)
    if version == 'waveshare':
        e = last['easy']
        if e['state'] is not None:
            total += 1
            if e['state'] != state:
                diverge += 1
                log(f"DIVERGENZ #{diverge}/{total}: "
                    f"easy=State{e['state']} ({e['trace'][:50]}) | "
                    f"waveshare=State{state} ({trace[:50]})")
            if total % 60 == 0:
                summary()

    last[version]['state'] = state
    last[version]['trace'] = trace

def on_signal(sig, frame):
    summary()
    sys.exit(0)

signal.signal(signal.SIGTERM, on_signal)
signal.signal(signal.SIGINT, on_signal)

try:
    db_connect()
    log("DB verbunden (pv_decision_log)")
except Exception as e:
    log(f"DB-Connect fehlgeschlagen (wird je Write erneut versucht): {e}")

log("Start Sammler+Vergleich — fox2db/easy/state vs sofar/state + soyo/calc → pv_decision_log")

client = mqtt.Client(client_id="ws_compare_collector", clean_session=True)
client.on_message = on_message
client.connect(BROKER, 1883, 60)
client.subscribe('fox2db/easy/state')
client.subscribe('sofar/state')
client.subscribe('soyo/calc')

try:
    client.loop_forever()
except KeyboardInterrupt:
    pass
finally:
    summary()
    client.disconnect()
    log("Sammler beendet.")
