#!/usr/bin/env python3
"""
waveshare_compare.py — Vergleich + DB-Sammler: fox2dbEasy (Pi) vs Waveshare ESP32.

Subscriptions:
  fox2db/easy/state   → easy-Entscheidung (version v2.0-Port)
  sofar/state         → waveshare-Entscheidung (volle Daten inkl. trace)

- Schreibt JEDE Entscheidung beider Engines in MariaDB pv_decision_log
  (version='easy' / 'waveshare') — neben prod (v2.9-Py) in derselben Tabelle.
- Meldet zusätzlich Abweichungen (easy.state != waveshare.state) ins /tmp-Log.
Läuft endlos bis SIGTERM/SIGINT.
"""
import json, time, datetime, signal, sys, re
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
total   = 0
diverge = 0
db_ok   = 0
db_err  = 0
latest_auto = None   # ESP auto-Flag (1=ESP regelt selbst, 0=externer Controller) aus sofar/state
start_ts = time.monotonic()

_conn = None

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

# ── Auswertung ────────────────────────────────────────────────────────────────
def summary():
    elapsed = int(time.monotonic() - start_ts)
    pct = f"{diverge/total*100:.1f}%" if total else "n/a"
    log(f"SUMMARY: {total} Vergleiche, {diverge} Abweichungen ({pct}) "
        f"nach {elapsed//3600}h{elapsed%3600//60}m | DB ok={db_ok} err={db_err}")

def on_message(client, userdata, msg):
    global total, diverge, latest_auto
    try:
        j = json.loads(msg.payload.decode())
    except Exception:
        return

    if msg.topic == 'fox2db/easy/state':
        version = 'easy'
    elif msg.topic == 'sofar/state':
        version = 'waveshare'
    else:
        return

    # auto-Flag: waveshare liefert es direkt; easy bekommt den zuletzt bekannten Wert.
    if version == 'waveshare' and j.get('auto') is not None:
        latest_auto = int(j.get('auto'))
    auto = latest_auto

    # DB-Schreiben (jede Entscheidung, beide Engines) inkl. auto-Flag
    write_decision(version, j, auto)

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

log("Start Sammler+Vergleich — fox2db/easy/state vs sofar/state → pv_decision_log")

client = mqtt.Client(client_id="ws_compare_collector", clean_session=True)
client.on_message = on_message
client.connect(BROKER, 1883, 60)
client.subscribe('fox2db/easy/state')
client.subscribe('sofar/state')

try:
    client.loop_forever()
except KeyboardInterrupt:
    pass
finally:
    summary()
    client.disconnect()
    log("Sammler beendet.")
