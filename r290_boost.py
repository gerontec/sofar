#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Cron: jede Minute. Boost AN (P01+P02 → 1K, P03/WW-Soll → 55°C) bei
#   - Einspeisung >= SURPLUS_THRESHOLD (6 kW, 2-min-Mittel; bis v1.11 9 kW) — reiner Ueberschuss, oder
#   - Peak >= EXCESS_THRESHOLD (18 kW) bzw. Abregel-Anforderung der ESP (do4).
# Der Ueberschuss-Pfad ruht, solange das Klarhimmel-Peak-Modell fuer heute einen Peak
# > 20 kW erwartet und dessen Fenster laeuft — dann wirkt nur die alte Logik.
# Peak-Modell seit v1.10 (09.10.2026): batmonitor auf der Venus (Pi .119) publiziert es
# retained auf batmonitor/peak_model (gleiche Felder wie frueher die Waveshare-ESP in
# sofar/state; die ESP ist seit 08.10.2026 abgebaut, ihr Cache-File veraltete).
# Momentan-PCC seit v1.11: der 4-s-Strom von sofar_fast.py (nicht retained, source
# sofar_fast, plausible 1) statt des bis zu 60 s alten retained Werts von pivot2db; fehlt
# der 2-min-Mittelwert in inverter_data (sofar.sh uebersprungen), gilt der Live-Wert.
# Beides nur wenn SOC > SOC_MIN_BOOST. Boost AUS (10K/6K, 48°C) erst, wenn Netzbezug plus
# Akku-Entladung 0,6 kW uebersteigen: Bilanz PCC + Sofar Bat1 + AC-in der Multis < -0,6 kW
# (2-min-Mittel; v1.15, User 09.10.2026: den Boost nicht aus dem Akku bedienen — der Sofar
# haelt seinen PCC mit Bat1 auf 0, im Sommer decken auch die Victron-Stacks die WP).
# v1.13/1.14: nur PCC < -0,2/-0,6 kW, bis v1.12 < 4 kW: die eigene Leistung der WP steckt im PCC,
# bei 4 kW schaltete sich der Boost selbst ab (User 09.10.2026: nur abschalten, sobald
# Strom gekauft werden muss).
# PCC-Vorzeichen: positiv = Einspeisung ins Netz.
# Urlaubsschaltung: steht P03 (WW-Soll) von Hand unter URLAUB_P03_MAX (36°C),
# schreibt das Script gar nichts — kein Boost, auch kein Zuruecksetzen auf 48°C.
# v1.16 (10.10.2026): evening window of r290_evening.py (19:00-20:29) — BOOST AUS restores
# P02 to EVENING_P02 (2 K) instead of NORMAL_P02 there, so both scripts agree.
# v1.17 (10.10.2026, user: boost before DO4): the peak path (PCC >= 18 kW live or 2-min mean, or a
# DO4 pulse in the last DO4_WINDOW_MIN) starts the boost regardless of SOC — that export would be
# shed anyway; SOC_MIN_BOOST only guards the pure surplus path. The DO4 pulse comes from
# batmonitor/do4_last (retained, batmonitor 0.50-c fires DO4 only after 2 min > 20 kW, so this
# minute cron gets its chance); pv_relay_events (ESP era, no longer written) and sofar/state
# (ESP removed 08.10.2026) are gone.

VERSION = "1.17"

from db_config import get_db_connection
import sys
import os
import time
import subprocess
import signal
import logging
import json
from logging.handlers import RotatingFileHandler
from r290_evening import in_window as evening_window, EVENING_P02

SCRIPT_TIMEOUT = 55  # Sekunden — self-timeout < rs485_batch-Limit (59s), Reserve für MQTT + Modbus-Retries

def _timeout_handler(signum, frame):
    raise SystemExit(f"TIMEOUT: Script lief länger als {SCRIPT_TIMEOUT}s")

signal.signal(signal.SIGALRM, _timeout_handler)
signal.alarm(SCRIPT_TIMEOUT)

_handler = RotatingFileHandler("/tmp/r290_boost.log", maxBytes=22528, backupCount=1)
_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
log = logging.getLogger("r290_boost")
log.setLevel(logging.INFO)
log.addHandler(_handler)
log.propagate = False

# === KONFIGURATION ===
PORT              = '/dev/ttyUSB33'
BAUDRATE          = 9600
SLAVE_ID          = 1
REG_P01           = 0x00E7          # Re-start Temperature difference Heating/Cooling
REG_P02           = 0x00E8          # Re-start Temperature difference Hot Water Mode
REG_P03           = 0x00BE          # Warmwasser-Solltemperatur
BOOST_DELTA       = 2               # K — P01+P02 Einschalthysterese bei Überschuss;
                                    #     2 ist das Minimum der dokumentierten Spanne
                                    #     2–18 °C (Registerkarte 0x00E7/0x00E8). Vorher 1,
                                    #     also ausserhalb der Spezifikation.
NORMAL_DELTA      = 10              # K — P01 Einschalthysterese normal
NORMAL_P02        = 6               # K — P02 Normalwert (aus Backup bestätigt: 6K)
BOOST_TEMP        = 55              # °C — WW-Solltemperatur bei Überschuss
NORMAL_TEMP       = 48              # °C — WW-Solltemperatur normal (kein Überschuss)
URLAUB_P03_MAX    = 36              # °C — P03 darunter = Urlaubsschaltung: nichts schreiben
EXCESS_THRESHOLD    = 18.0   # kW — Boost AN (Peak-Begrenzung)
SURPLUS_THRESHOLD   = 6.0    # kW — Boost AN bei reinem Ueberschuss (Akku voll), 9 → 6 kW (User 09.10.2026),
                             #        unabhaengig von Peak/Abregelung. 2-min-Mittel.
MODEL_TOPIC         = 'batmonitor/peak_model'  # retained, 1x/min von batmonitor (Venus Pi .119):
                             #        {"ts", "peak_h", "win_end_h", "ratio_now", ...}
MODEL_WAIT          = 5      # s — Wartezeit auf die retained Message (kommt normal in ms)
MODEL_MAX_AGE_S     = 600    # s — aelterer Modellwert gilt als nicht vertrauenswuerdig
# Spiegeln die Waechter, mit denen der ESP/fox2dbEasy seine eigene Peak-Vorhaltung
# (Ladesperre) absichert: LADESPERRE_MONTH_FROM/TO und LADESPERRE_NOW_RATIO.
PEAK_MONTH_FROM     = 5      # Mai  — ausserhalb Mai-August keine Peak-Vorhaltung,
PEAK_MONTH_TO       = 8      # Aug.   dort regelt allein do4 (>20 kW) ab
PEAK_BADWX_RATIO    = 0.20   # Momentan-Ratio, ab der Bewoelkung den Klarhimmel-Peak widerlegt
BOOST_OFF_THRESHOLD = -0.6   # kW — Boost AUS, wenn Netzbezug + Akku-Entladung (Sofar Bat1 + Multis)
                             #      mehr als 600 W (2-min-Mittel, User 09.10.2026). batmonitor regelt den
                             #      PCC auf 0 W, +-100 W
VICTRON_MAX_AGE_S   = 180    # s — pv_victron (victron2db.py, 1x/min) aelter: Multis zaehlen als 0
STALE_MINUTES       = 5      # PCC-Wert darf max. 5 Minuten alt sein
SMOOTH_MINUTES      = 2      # Glättung: Durchschnitt über letzte N Minuten
SOC_MIN_BOOST       = 89     # % — Boost nur starten wenn Akku-SOC > diesem Wert

# Registerschreiben laeuft ueber r290_write (C), nicht ueber pymodbus.
# Grund (gemessen 2026-09-03, Slave 1, 0x00E7): die R290-Firmware beantwortet FC16
# sporadisch mit einem um ein Byte verkuerzten Frame
#     RX 01 10 00 E7 00 56 F0   statt   01 10 00 E7 00 02 F1 FF
# — die CRC passt jeweils zum verkuerzten Rumpf, die Firmware baut ihn selbst falsch.
# pymodbus wartet auf das fehlende Byte und wirft nach drei Retries (12 s)
# ModbusIOException; daran starb dieses Script bei jedem Boost-Wechsel mit rc=1.
# FC6 je Register antwortet in allen Messungen sofort und korrekt, genau das macht
# r290_write. Der Helfer liest nach jedem Schreiben zur Kontrolle zurueck.
R290_WRITE_BIN      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "r290_write")
WRITE_TIMEOUT       = 15     # s — Aussenwaechter um r290_write (intern max. 3 Versuche)

ACTIVE_THRESHOLD = EXCESS_THRESHOLD

def _proc_cmdline(pid):
    try:
        with open(f'/proc/{pid}/cmdline', 'rb') as f:
            return f.read().decode(errors='replace').replace('\x00', ' ').strip()
    except Exception:
        return f"PID {pid}"

def _proc_ppid(pid):
    try:
        with open(f'/proc/{pid}/status') as f:
            for line in f:
                if line.startswith('PPid:'):
                    return int(line.split()[1])
    except Exception:
        pass
    return None

def _caller():
    ppid = os.getppid()
    chain = [_proc_cmdline(ppid)]
    gppid = _proc_ppid(ppid)
    if gppid and gppid > 1:
        chain.append(_proc_cmdline(gppid))
    return ' ← '.join(chain)

def read_pcc_from_db():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT AVG(ActivePower_PCC_Total),
                       MAX(timestamp),
                       COUNT(*),
                       MAX(ActivePower_PCC_Total),
                       AVG(Power_Bat1)
                FROM inverter_data
                WHERE ActivePower_PCC_Total IS NOT NULL
                  AND timestamp >= NOW() - INTERVAL %s MINUTE
            """, (SMOOTH_MINUTES,))
            row = cur.fetchone()
        conn.close()
        if not row or row[0] is None:
            return None, None, None
        avg, latest_ts, n, latest = float(row[0]), str(row[1]), int(row[2]), float(row[3])
        bat1 = float(row[4]) if row[4] is not None else None
        log.info(f"PCC glatt={avg:.2f} kW (n={n}, max={latest:.2f}, ts={latest_ts}), Bat1 glatt={bat1} kW")
        return avg, latest_ts, bat1
    except Exception as e:
        log.error(f"DB Fehler: {e}")
        return None, None, None

def read_multis_from_db():
    """AC-in der drei MultiPlus (kW, + = sie laden aus dem AC-Netz) aus pv_victron, 2-min-Mittel.
    None, wenn die letzte Zeile aelter als VICTRON_MAX_AGE_S ist."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT AVG(COALESCE(ac_in_l1_w,0) + COALESCE(ac_in_l2_w,0) + COALESCE(ac_in_l3_w,0)) / 1000,
                       TIMESTAMPDIFF(SECOND, MAX(ts), NOW()), COUNT(*)
                FROM pv_victron
                WHERE ts >= NOW() - INTERVAL %s MINUTE
            """, (SMOOTH_MINUTES + 1,))
            row = cur.fetchone()
        conn.close()
        if not row or row[0] is None or row[1] is None or row[1] > VICTRON_MAX_AGE_S:
            log.warning(f"pv_victron: kein Wert juenger als {VICTRON_MAX_AGE_S}s — Multis zaehlen als 0")
            return None
        log.info(f"Multis AC-in glatt={float(row[0]):.2f} kW (n={row[2]}, age={row[1]}s)")
        return float(row[0])
    except Exception as e:
        log.error(f"DB Fehler pv_victron: {e}")
        return None

def read_soc_from_db():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT Coulomb FROM pv_ebox2
                WHERE Coulomb IS NOT NULL AND ts >= NOW() - INTERVAL %s MINUTE
                ORDER BY id DESC LIMIT 1
            """, (STALE_MINUTES,))
            row = cur.fetchone()
        conn.close()
        if not row:
            return None
        return int(row[0])
    except Exception as e:
        log.error(f"DB Fehler SOC: {e}")
        return None

def read_do4_last():
    """True if batmonitor fired a DO4 pulse (FoxESS shedding, PCC > 20 kW) within DO4_WINDOW_MIN:
    retained batmonitor/do4_last {"ts", "pcc_w"}."""
    import paho.mqtt.client as _mqtt
    result = {}
    def _on_msg(c, u, msg):
        try:
            result.update(json.loads(msg.payload.decode()))
        except Exception:
            pass
        c.disconnect()
    c = _mqtt.Client()
    c.on_message = _on_msg
    try:
        c.connect(MQTT_BROKER, 1883, 10)
        c.subscribe(DO4_LAST_TOPIC)
        c.loop_start()
        deadline = time.monotonic() + MODEL_WAIT
        while not result and time.monotonic() < deadline:
            time.sleep(0.05)
        c.loop_stop()
        c.disconnect()
    except Exception as e:
        log.warning(f'MQTT {DO4_LAST_TOPIC} nicht erreichbar: {e}')
        return False
    ts = result.get('ts')
    if not isinstance(ts, (int, float)):
        return False
    age = time.time() - ts
    if age <= DO4_WINDOW_MIN * 60:
        log.info(f"DO4 von batmonitor vor {age:.0f} s (PCC {result.get('pcc_w')} W)")
        return True
    return False

def read_p01_p02_p03_from_db():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p01_restart_diff, p03_hw_setpoint, compressor_actual_frequency,
                       p02_hw_restart_diff
                FROM heatpr290
                WHERE p01_restart_diff IS NOT NULL AND p03_hw_setpoint IS NOT NULL
                ORDER BY ts DESC LIMIT 1
            """)
            row = cur.fetchone()
        conn.close()
        if row:
            freq = float(row[2]) if row[2] is not None else None
            p02  = int(row[3])   if row[3] is not None else None
            return int(row[0]), int(row[1]), freq, p02
        return None, None, None, None
    except Exception as e:
        log.error(f"DB Fehler P01/P02/P03: {e}")
        return None, None, None, None

def _c_write(start_reg, values, label, retries=2):
    """Schreibt ueber r290_write (C). Der Helfer macht FC6 je Register, drei
    Versuche, und liest zur Kontrolle zurueck. rc 0 = geschrieben und verifiziert."""
    cmd = [R290_WRITE_BIN, PORT, str(SLAVE_ID), f"0x{start_reg:04X}"] + [str(int(v)) for v in values]
    for attempt in range(retries + 1):
        try:
            p = subprocess.run(cmd, capture_output=True, text=True, timeout=WRITE_TIMEOUT)
        except subprocess.TimeoutExpired:
            log.warning(f"{label}: r290_write Zeitueberschreitung (Versuch {attempt + 1})")
            continue
        meldung = " | ".join(x.strip() for x in (p.stdout, p.stderr) if x and x.strip())
        if p.returncode == 0:
            log.info(f"{label}: {meldung}")
            return True
        log.warning(f"{label}: r290_write rc={p.returncode} (Versuch {attempt + 1}): {meldung}")
        if attempt < retries:
            time.sleep(0.4)
    log.error(f"Schreiben {label}={values} @ 0x{start_reg:04X} endgueltig fehlgeschlagen")
    return False


def write_reg(client, reg, value, label, retries=2):
    return _c_write(reg, [value], label, retries)


def write_regs_bulk(client, start_reg, values, label, retries=2):
    """Zusammenhaengender Registerblock. r290_write schreibt ihn Register fuer Register."""
    return _c_write(start_reg, values, label, retries)


MQTT_BROKER   = '192.168.178.218'
DO4_LAST_TOPIC = 'batmonitor/do4_last'   # retained {"ts", "pcc_w"}: letzter DO4-Puls von batmonitor (0.50-c)
MQTT_INV_TOPIC = 'inverter/power_grid_exchange/json'  # pivot2db.py, qos1/retain → PCC kommt sofort beim Subscribe
PCC_PEAK_W    = 20000            # W — PCC_OVER_20KW / Peak-Schwelle der ESP (do4)
INV_GET_WAIT  = 6                # s — Wartezeit auf eine Live-Message von sofar_fast (alle ~4 s)
INV_PCC_STALE_S = 150            # s — retained PCC darf max. so alt sein (timestamp-Feld)
DO4_WINDOW_MIN = 3               # min — Fallback: do4-Puls in pv_relay_events dieser Spanne zählt als Trigger

LIVE = {}    # Bat1 der zuletzt gelesenen Inverter-Message (Ersatz fuer den 2-min-Wert)

def read_inverter_pcc_mqtt():
    """Momentaner PCC (kW) vom Inverter-Topic. Bevorzugt die naechste Live-Message von
    sofar_fast.py (alle ~4 s, nicht retained, source sofar_fast, plausible 1); die
    retained Message von pivot2db.py (1x/min) gilt nur, wenn in INV_GET_WAIT keine
    Live-Message kommt. Ungeglaettet: faengt scharfe Peaks, die der 2-min-DB-Durchschnitt
    verschluckt. Freshness ueber das timestamp-Feld.
    Gibt PCC in kW zurueck oder None (nicht erreichbar / veraltet)."""
    import paho.mqtt.client as _mqtt
    from datetime import datetime
    result, retained = {}, {}
    def _on_msg(c, u, msg):
        try:
            j = json.loads(msg.payload.decode())
            r = {'pcc': float(j.get('ActivePower_PCC_Total')), 'ts': j.get('timestamp'),
                 'bat1': j.get('Power_Bat1')}
        except Exception:
            return
        if msg.retain:
            retained.update(r, src='pivot2db (retained)')
            return
        if j.get('source') != 'sofar_fast' or j.get('plausible', 1) != 1:
            return
        result.update(r, src='sofar_fast (live)')
        c.disconnect()
    c = _mqtt.Client()
    c.on_message = _on_msg
    try:
        c.connect(MQTT_BROKER, 1883, 10)
        c.subscribe(MQTT_INV_TOPIC)
        c.loop_start()
        deadline = time.monotonic() + INV_GET_WAIT
        while not result and time.monotonic() < deadline:
            time.sleep(0.05)
        c.loop_stop()
        c.disconnect()
    except Exception as e:
        log.warning(f'MQTT {MQTT_INV_TOPIC} nicht erreichbar: {e}')
        return None
    if 'pcc' not in result:
        if 'pcc' not in retained:
            log.info(f'{MQTT_INV_TOPIC}: keine Message in {INV_GET_WAIT}s')
            return None
        log.info(f'{MQTT_INV_TOPIC}: keine Live-Message von sofar_fast in {INV_GET_WAIT}s — retained Wert')
        result = retained
    age = None
    if result.get('ts'):
        try:
            age = (datetime.now() - datetime.fromisoformat(result['ts'])).total_seconds()
        except Exception:
            age = None
    if age is not None and age > INV_PCC_STALE_S:
        log.warning(f"Inverter-PCC veraltet: {age:.0f}s > {INV_PCC_STALE_S}s (ts={result['ts']}) — ignoriert")
        return None
    _age_str = f', age={age:.0f}s' if age is not None else ''
    log.info(f"Inverter-PCC {result['src']} = {result['pcc']:.2f} kW, Bat1 = {result.get('bat1')} kW{_age_str}")
    LIVE['bat1'] = result.get('bat1')
    return result['pcc']


class _Bus:
    """Platzhalter fuer den frueheren pymodbus-Client: den Port oeffnet und
    schliesst jetzt r290_write bei jedem Aufruf selbst."""

    def close(self):
        pass


def open_modbus():
    if not os.access(R290_WRITE_BIN, os.X_OK):
        log.error(f"Schreib-Helfer fehlt oder ist nicht ausfuehrbar: {R290_WRITE_BIN}")
        return None
    if not os.path.exists(PORT):
        log.error(f"RS485-Port nicht vorhanden: {PORT}")
        return None
    return _Bus()

def read_peak_model_mqtt():
    """Retained Peak-Modell von batmonitor (MODEL_TOPIC) als dict, None wenn keins kommt."""
    import paho.mqtt.client as _mqtt
    result = {}
    def _on_msg(c, u, msg):
        try:
            result.update(json.loads(msg.payload.decode()))
        except Exception:
            pass
        c.disconnect()
    c = _mqtt.Client()
    c.on_message = _on_msg
    try:
        c.connect(MQTT_BROKER, 1883, 10)
        c.subscribe(MODEL_TOPIC)
        c.loop_start()
        deadline = time.monotonic() + MODEL_WAIT
        while not result and time.monotonic() < deadline:
            time.sleep(0.05)
        c.loop_stop()
        c.disconnect()
    except Exception as e:
        log.warning(f'MQTT {MODEL_TOPIC} nicht erreichbar: {e}')
        return None
    return result or None


def read_peak_ahead():
    """(peak_ahead, Begruendung) aus dem Peak-Modell von batmonitor (MODEL_TOPIC).

    peak_ahead heisst: das Modell erwartet fuer heute einen DC-Peak > 20 kW
    (peak_h >= 0) und das Fenster ist noch nicht vorbei (aktuelle Stunde
    <= win_end_h). Solange das gilt, darf NUR die alte Logik wirken — also
    Peak >= EXCESS_THRESHOLD bzw. do4 —, denn der Ueberschuss-Boost wuerde den
    Warmwasserpuffer leerfahren, der fuer die Abregelung gebraucht wird.

    Fehlt die Message oder ist sie aelter als MODEL_MAX_AGE_S, wird Peak-Gefahr
    ANGENOMMEN. Das ist die sichere Richtung: es schaltet nur die neue
    Zusatzschwelle ab, die alte Logik bleibt unberuehrt.
    """
    m = read_peak_model_mqtt()
    if m is None:
        return True, f"kein Peak-Modell auf {MODEL_TOPIC} — Peak-Gefahr angenommen"
    age = time.time() - float(m.get("ts", 0))
    if age > MODEL_MAX_AGE_S:
        return True, f"Modellwerte {age/60:.0f} min alt — Peak-Gefahr angenommen"
    now   = time.localtime()
    month = now.tm_mon
    if not (PEAK_MONTH_FROM <= month <= PEAK_MONTH_TO):
        return False, (f"Monat {month} ausserhalb {PEAK_MONTH_FROM}-{PEAK_MONTH_TO} — "
                       f"keine Peak-Vorhaltung, do4 regelt ab")
    peak_h    = m.get("peak_h")
    win_end_h = m.get("win_end_h")
    if not isinstance(peak_h, int) or peak_h < 0:
        return False, "Modell erwartet heute keinen Peak > 20 kW"
    end  = win_end_h if isinstance(win_end_h, int) and win_end_h >= 0 else peak_h
    hour = now.tm_hour
    if hour > end:
        return False, f"Peak-Fenster vorbei (bis {end}:59, jetzt {hour}:xx)"
    # Klarhimmel-Schranke gegen das Ist-Wetter halten: liegt die Momentanleistung
    # deutlich unter dem Modell, ist der 20-kW-Peak fuer heute widerlegt.
    # ratio_now < 0 heisst "nicht berechenbar" und darf die Gefahr nicht aufheben.
    rn = m.get("ratio_now")
    if isinstance(rn, (int, float)) and rn >= 0 and rn >= PEAK_BADWX_RATIO:
        return False, (f"Wetter widerlegt den Klarhimmel-Peak "
                       f"(ratio_now={rn:.2f} >= {PEAK_BADWX_RATIO})")
    return True, f"Modell: Peak {peak_h}:00, Fenster bis {end}:59, jetzt {hour}:xx, ratio_now={rn}"


# === MAIN ===
_deploy = time.strftime("%Y-%m-%d %H:%M", time.localtime(os.path.getmtime(__file__)))
log.info(f"Start v{VERSION} (deployed {_deploy}): {_caller()}")

pcc, ts, bat1 = read_pcc_from_db()
# Momentaner PCC (sofar_fast live) — auch Ersatz, wenn inverter_data keinen 2-min-Wert hat
inst_pcc = read_inverter_pcc_mqtt()
if pcc is None:
    if inst_pcc is None:
        log.info("PCC: kein aktueller Wert in DB und keiner per MQTT — keine Aktion")
        sys.exit(0)
    pcc = inst_pcc
    bat1 = LIVE.get('bat1')
    log.info(f"PCC: kein 2-min-Wert in DB — Momentanwert {pcc:.2f} kW gilt")
# Bilanz fuer BOOST AUS: Netz + Akkus (+ = Einspeisung / Laden). Negativ heisst, Netz oder
# Akkus decken Haus + WP — der Boost soll weder Strom kaufen noch die Akkus leeren.
multis = read_multis_from_db()
balance = pcc + (float(bat1) if bat1 is not None else 0.0) + (multis if multis is not None else 0.0)
log.info(f"Bilanz PCC {pcc:.2f} + Bat1 {bat1} + Multis {multis} = {balance:.2f} kW "
         f"(Boost AUS unter {BOOST_OFF_THRESHOLD} kW)")

soc = read_soc_from_db()
if soc is None:
    log.warning("SOC: kein aktueller Wert in pv_ebox2 — Boost-Start gesperrt")
else:
    log.info(f"SOC={soc}%")

current_p01, current_p03, current_freq, current_p02 = read_p01_p02_p03_from_db()
if current_p01 is None or current_p03 is None:
    log.error("P01/P03 aus DB nicht lesbar — keine Aktion")
    sys.exit(1)
_freq_str = f", Hz={current_freq:.0f}" if current_freq is not None else ""
_p02_str  = f"/{current_p02}K" if current_p02 is not None else "/?K"

# Urlaubsschaltung: von Hand niedrig gestellter WW-Soll bleibt unangetastet.
# Muss vor jedem Schreibpfad stehen — auch BOOST AUS wuerde P03 sonst auf 48°C heben.
if current_p03 < URLAUB_P03_MAX:
    log.info(f"Urlaubsschaltung: P03={current_p03}°C < {URLAUB_P03_MAX}°C — kein Boost, "
             f"keine Aenderung (P01={current_p01}K, P02={_p02_str}, PCC={pcc:.2f} kW, SOC={soc}%)")
    sys.exit(0)

boost_is_active = (
    current_p01 == BOOST_DELTA and
    current_p03 == BOOST_TEMP and
    (current_p02 is None or current_p02 == BOOST_DELTA)
)

# Momentaner PCC vom retained Inverter-Topic (instant) — fängt Peaks, die der
# 2-min-DB-Durchschnitt verschluckt. do4-Puls der ESP über DB-Fallback (instant)
# bzw. best-effort sofar/state (nur wenn noch kein Peak erkannt → spart Lausch-Wartezeit).
peak_pcc     = (inst_pcc is not None and inst_pcc >= EXCESS_THRESHOLD)
do4_trigger  = read_do4_last()
ws_trigger   = do4_trigger or peak_pcc
# Ueberschuss-Trigger: geladener Akku + anhaltende Einspeisung reichen aus,
# ohne dass eine Abregelung (do4/Peak) vorliegen muss.
peak_ahead, peak_why = read_peak_ahead()
surplus_pcc  = (pcc >= SURPLUS_THRESHOLD) and not peak_ahead
trigger_on   = (pcc >= ACTIVE_THRESHOLD) or ws_trigger or surplus_pcc
log.info(f"Peak-Gefahr (Peak-Modell batmonitor): {peak_ahead} — {peak_why}")
if do4_trigger:
    trigger_src = 'ESP:do4'
elif peak_pcc:
    trigger_src = f'PCC_inst={inst_pcc:.2f} kW (MQTT)'
elif pcc >= ACTIVE_THRESHOLD:
    trigger_src = f'PCC={pcc:.2f} kW'
elif surplus_pcc:
    trigger_src = f'Ueberschuss PCC={pcc:.2f} kW >= {SURPLUS_THRESHOLD} kW'
else:
    trigger_src = f'kein Trigger (PCC={pcc:.2f} kW)'

log.info(f"Schwellen: Peak {ACTIVE_THRESHOLD} kW / Ueberschuss {SURPLUS_THRESHOLD} kW | "
         f"do4={do4_trigger} peak_inst={peak_pcc} surplus={surplus_pcc} → trigger={trigger_on} ({trigger_src})")

# Peak-Pfad (v1.17): diese Einspeisung wuerde abgeregelt — Boost unabhaengig vom SOC.
peak_path = (pcc >= ACTIVE_THRESHOLD) or ws_trigger
if trigger_on:
    if not boost_is_active:
        if not peak_path and (soc is None or soc <= SOC_MIN_BOOST):
            log.info(f"BOOST gesperrt: SOC={soc}% ≤ {SOC_MIN_BOOST}% (PCC={pcc:.2f} kW)")
        else:
            client = open_modbus()
            if client:
                ok12 = write_regs_bulk(client, REG_P01, [BOOST_DELTA, BOOST_DELTA], "P01+P02")
                ok3  = write_reg(client, REG_P03, BOOST_TEMP, "P03")
                client.close()
                if ok12 and ok3:
                    log.info(f"BOOST AN: P01 {current_p01}→{BOOST_DELTA}K, P02 {_p02_str}→{BOOST_DELTA}K, P03 {current_p03}→{BOOST_TEMP}°C ({trigger_src}, SOC={soc}%{_freq_str})")
                    print(f"BOOST AN: P01 {current_p01}→{BOOST_DELTA}K | P02 {_p02_str}→{BOOST_DELTA}K | P03 {current_p03}→{BOOST_TEMP}°C | {trigger_src} | SOC={soc}%{_freq_str}")
                else:
                    # Schreiben endgueltig gescheitert: rc!=0, damit rs485_batch alarmiert.
                    log.error(f"BOOST AN fehlgeschlagen (P01/P02 ok={ok12}, P03 ok={ok3})")
                    print(f"BOOST AN fehlgeschlagen: P01/P02 ok={ok12}, P03 ok={ok3}", file=sys.stderr)
                    sys.exit(1)
    else:
        log.info(f"BOOST aktiv: P01={current_p01}K, P02={_p02_str}, P03={current_p03}°C, {trigger_src}, SOC={soc}%{_freq_str}")
elif balance < BOOST_OFF_THRESHOLD:
    if evening_window():
        NORMAL_P02 = EVENING_P02
        log.info(f"Abendfenster r290_evening: P02-Normalwert {EVENING_P02}K")
    normal_mismatch = (
        current_p01 != NORMAL_DELTA or
        current_p03 != NORMAL_TEMP or
        (current_p02 is not None and current_p02 != NORMAL_P02)
    )
    if normal_mismatch:
        client = open_modbus()
        if client:
            ok12 = write_regs_bulk(client, REG_P01, [NORMAL_DELTA, NORMAL_P02], "P01+P02")
            ok3  = write_reg(client, REG_P03, NORMAL_TEMP, "P03")
            client.close()
            if ok12 and ok3:
                log.info(f"BOOST AUS: P01 {current_p01}→{NORMAL_DELTA}K, P02 {_p02_str}→{NORMAL_P02}K, P03 {current_p03}→{NORMAL_TEMP}°C (PCC={pcc:.2f} kW, Bilanz={balance:.2f} kW{_freq_str})")
                print(f"BOOST AUS: P01 {current_p01}→{NORMAL_DELTA}K | P02 {_p02_str}→{NORMAL_P02}K | P03 {current_p03}→{NORMAL_TEMP}°C | PCC={pcc:.2f} kW | Bilanz={balance:.2f} kW{_freq_str}")
            else:
                # Schreiben endgueltig gescheitert: rc!=0, damit rs485_batch alarmiert.
                log.error(f"BOOST AUS fehlgeschlagen (P01/P02 ok={ok12}, P03 ok={ok3})")
                print(f"BOOST AUS fehlgeschlagen: P01/P02 ok={ok12}, P03 ok={ok3}", file=sys.stderr)
                sys.exit(1)
    else:
        log.info(f"Normal: P01={current_p01}K, P02={_p02_str}, P03={current_p03}°C, PCC={pcc:.2f} kW{_freq_str}")
else:
    # Hysterese-Bereich: keine Änderung
    if boost_is_active:
        log.info(f"BOOST Hysterese: P01={current_p01}K, P02={_p02_str}, P03={current_p03}°C, PCC={pcc:.2f} kW{_freq_str}")
    else:
        log.info(f"Normal: P01={current_p01}K, P02={_p02_str}, P03={current_p03}°C, PCC={pcc:.2f} kW{_freq_str}")
