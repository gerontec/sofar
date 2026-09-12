#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Cron: jede Minute. Boost AN (P01+P02 → 1K, P03/WW-Soll → 55°C) bei
#   - Einspeisung >= SURPLUS_THRESHOLD (9 kW, 2-min-Mittel) — reiner Ueberschuss, oder
#   - Peak >= EXCESS_THRESHOLD (18 kW) bzw. Abregel-Anforderung der ESP (do4).
# Der Ueberschuss-Pfad ruht, solange das ESP32-S3-Modell fuer heute einen Peak
# > 20 kW erwartet und dessen Fenster laeuft — dann wirkt nur die alte Logik.
# Beides nur wenn SOC > SOC_MIN_BOOST. Boost AUS (10K/6K, 48°C) erst bei < 4 kW.
# PCC-Vorzeichen: positiv = Einspeisung ins Netz.

VERSION = "1.8"

from db_config import get_db_connection
import sys
import os
import time
import subprocess
import signal
import logging
import json
from logging.handlers import RotatingFileHandler

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
EXCESS_THRESHOLD    = 18.0   # kW — Boost AN (Peak-Begrenzung)
SURPLUS_THRESHOLD   = 9.0    # kW — Boost AN bei reinem Ueberschuss (Akku voll),
                             #        unabhaengig von Peak/Abregelung. 2-min-Mittel.
MODEL_CACHE         = '/tmp/sofar_model_last.json'  # ESP32-S3-Modellwerte, geschrieben
                             #        von waveshare_compare.py (sofar/state ist nicht retained)
MODEL_MAX_AGE_S     = 600    # s — aelterer Modellwert gilt als nicht vertrauenswuerdig
# Spiegeln die Waechter, mit denen der ESP/fox2dbEasy seine eigene Peak-Vorhaltung
# (Ladesperre) absichert: LADESPERRE_MONTH_FROM/TO und LADESPERRE_NOW_RATIO.
PEAK_MONTH_FROM     = 5      # Mai  — ausserhalb Mai-August keine Peak-Vorhaltung,
PEAK_MONTH_TO       = 8      # Aug.   dort regelt allein do4 (>20 kW) ab
PEAK_BADWX_RATIO    = 0.20   # Momentan-Ratio, ab der Bewoelkung den Klarhimmel-Peak widerlegt
BOOST_OFF_THRESHOLD = 4      # kW — Boost AUS wenn PCC < 4 kW
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
                       MAX(ActivePower_PCC_Total)
                FROM inverter_data
                WHERE ActivePower_PCC_Total IS NOT NULL
                  AND timestamp >= NOW() - INTERVAL %s MINUTE
            """, (SMOOTH_MINUTES,))
            row = cur.fetchone()
        conn.close()
        if not row or row[0] is None:
            return None, None
        avg, latest_ts, n, latest = float(row[0]), str(row[1]), int(row[2]), float(row[3])
        log.info(f"PCC glatt={avg:.2f} kW (n={n}, max={latest:.2f}, ts={latest_ts})")
        return avg, latest_ts
    except Exception as e:
        log.error(f"DB Fehler: {e}")
        return None, None

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

def read_do4_db_trigger():
    """Fallback zum nicht-retained sofar/state: prüft, ob die waveshare-ESP in den
    letzten DO4_WINDOW_MIN Minuten einen DO4-Puls (Abregelung PCC>20kW) gefeuert hat.
    Fängt Peaks, die der 60s-Topic-Takt zwischen zwei Cron-Läufen verpasst."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ts, reason FROM pv_relay_events
                WHERE relay='do4' AND ts >= NOW() - INTERVAL %s MINUTE
                ORDER BY ts DESC LIMIT 1
            """, (DO4_WINDOW_MIN,))
            row = cur.fetchone()
        conn.close()
        if not row:
            return False
        log.info(f"DB-DO4-Fallback: Puls @ {row[0]} ({row[1]})")
        return True
    except Exception as e:
        log.error(f"DB Fehler DO4: {e}")
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
MQTT_WS_TOPIC = 'sofar/state'    # waveshare ESP32-S3 (on-device) Status — NICHT retained, 1×/60s @ :47
MQTT_INV_TOPIC = 'inverter/power_grid_exchange/json'  # pivot2db.py, qos1/retain → PCC kommt sofort beim Subscribe
PCC_PEAK_W    = 20000            # W — PCC_OVER_20KW / Peak-Schwelle der ESP (do4)
WS_WAIT       = 3                # s — Best-effort-Lausch auf sofar/state (do4-Schnellpfad); DB-Fallback deckt sicher ab
INV_GET_WAIT  = 5                # s — Wartezeit auf retained Inverter-PCC (kommt normal in ms)
INV_PCC_STALE_S = 150            # s — retained PCC darf max. so alt sein (timestamp-Feld)
DO4_WINDOW_MIN = 3               # min — Fallback: do4-Puls in pv_relay_events dieser Spanne zählt als Trigger

def read_waveshare_trigger():
    """Liest sofar/state (waveshare ESP32-S3) → Abregel-Anforderung der ESP.
    Trigger wenn die ESP einen DO4-Puls fordert (do4==1) ODER PCC die
    Peak-Schwelle (>20 kW) erreicht. sofar/state ist NICHT retained und wird
    ca. alle 60 s publiziert → wir warten bis WS_WAIT s auf eine frische Message."""
    import paho.mqtt.client as _mqtt
    result = {}
    def _on_msg(c, u, msg):
        try:
            j = json.loads(msg.payload.decode())
            result['do4'] = bool(j.get('do4', 0))
            result['pcc'] = float(j.get('pcc', 0))
        except Exception:
            pass
        c.disconnect()
    c = _mqtt.Client()
    c.on_message = _on_msg
    try:
        c.connect(MQTT_BROKER, 1883, 10)
        c.subscribe(MQTT_WS_TOPIC)
        c.loop_start()
        deadline = time.monotonic() + WS_WAIT
        while not result and time.monotonic() < deadline:
            time.sleep(0.05)
        c.loop_stop()
        c.disconnect()
    except Exception as e:
        log.warning(f'MQTT {MQTT_WS_TOPIC} nicht erreichbar: {e}')
    if not result:
        log.info(f'waveshare {MQTT_WS_TOPIC}: keine Message in {WS_WAIT}s — kein Trigger')
        return False
    trig = result['do4'] or result['pcc'] >= PCC_PEAK_W
    log.info(f"waveshare do4={result['do4']} pcc={result['pcc']:.0f}W → trigger={trig}")
    return trig


def read_inverter_pcc_mqtt():
    """Liest den momentanen PCC vom retained Inverter-Topic (pivot2db.py, qos1/retain).
    Kommt sofort beim Subscribe → ungeglätteter PCC-Wert (kW), fängt scharfe Peaks,
    die der 2-min-DB-Durchschnitt verschluckt. Freshness über das timestamp-Feld.
    Gibt PCC in kW zurück oder None (nicht erreichbar / veraltet)."""
    import paho.mqtt.client as _mqtt
    from datetime import datetime
    result = {}
    def _on_msg(c, u, msg):
        try:
            j = json.loads(msg.payload.decode())
            result['pcc'] = float(j.get('ActivePower_PCC_Total'))
            result['ts']  = j.get('timestamp')
        except Exception:
            pass
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
        log.info(f'{MQTT_INV_TOPIC}: keine Message in {INV_GET_WAIT}s')
        return None
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
    log.info(f"Inverter-PCC (retained) = {result['pcc']:.2f} kW{_age_str}")
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

def read_peak_ahead():
    """(peak_ahead, Begruendung) aus dem ESP32-S3-Modell.

    peak_ahead heisst: das Modell erwartet fuer heute einen DC-Peak > 20 kW
    (peak_h >= 0) und das Fenster ist noch nicht vorbei (aktuelle Stunde
    <= win_end_h). Solange das gilt, darf NUR die alte Logik wirken — also
    Peak >= EXCESS_THRESHOLD bzw. do4 —, denn der Ueberschuss-Boost wuerde den
    Warmwasserpuffer leerfahren, der fuer die Abregelung gebraucht wird.

    Fehlt die Datei oder ist sie aelter als MODEL_MAX_AGE_S, wird Peak-Gefahr
    ANGENOMMEN. Das ist die sichere Richtung: es schaltet nur die neue
    Zusatzschwelle ab, die alte Logik bleibt unberuehrt.
    """
    try:
        with open(MODEL_CACHE) as f:
            m = json.load(f)
    except Exception as e:
        return True, f"Modellwerte nicht lesbar ({e}) — Peak-Gefahr angenommen"
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

pcc, ts = read_pcc_from_db()
if pcc is None:
    log.info("PCC: kein aktueller Wert in DB — keine Aktion")
    sys.exit(0)

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

boost_is_active = (
    current_p01 == BOOST_DELTA and
    current_p03 == BOOST_TEMP and
    (current_p02 is None or current_p02 == BOOST_DELTA)
)

# Momentaner PCC vom retained Inverter-Topic (instant) — fängt Peaks, die der
# 2-min-DB-Durchschnitt verschluckt. do4-Puls der ESP über DB-Fallback (instant)
# bzw. best-effort sofar/state (nur wenn noch kein Peak erkannt → spart Lausch-Wartezeit).
inst_pcc     = read_inverter_pcc_mqtt()
peak_pcc     = (inst_pcc is not None and inst_pcc >= EXCESS_THRESHOLD)
do4_trigger  = read_do4_db_trigger() or (not peak_pcc and read_waveshare_trigger())
ws_trigger   = do4_trigger or peak_pcc
# Ueberschuss-Trigger: geladener Akku + anhaltende Einspeisung reichen aus,
# ohne dass eine Abregelung (do4/Peak) vorliegen muss.
peak_ahead, peak_why = read_peak_ahead()
surplus_pcc  = (pcc >= SURPLUS_THRESHOLD) and not peak_ahead
trigger_on   = (pcc >= ACTIVE_THRESHOLD) or ws_trigger or surplus_pcc
log.info(f"Peak-Gefahr (ESP-Modell): {peak_ahead} — {peak_why}")
if do4_trigger:
    trigger_src = 'ESP:do4'
elif peak_pcc:
    trigger_src = f'PCC_inst={inst_pcc:.2f} kW (retained)'
elif pcc >= ACTIVE_THRESHOLD:
    trigger_src = f'PCC={pcc:.2f} kW'
elif surplus_pcc:
    trigger_src = f'Ueberschuss PCC={pcc:.2f} kW >= {SURPLUS_THRESHOLD} kW'
else:
    trigger_src = f'kein Trigger (PCC={pcc:.2f} kW)'

log.info(f"Schwellen: Peak {ACTIVE_THRESHOLD} kW / Ueberschuss {SURPLUS_THRESHOLD} kW | "
         f"do4={do4_trigger} peak_inst={peak_pcc} surplus={surplus_pcc} → trigger={trigger_on} ({trigger_src})")

if trigger_on:
    if not boost_is_active:
        if soc is None or soc <= SOC_MIN_BOOST:
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
elif pcc < BOOST_OFF_THRESHOLD:
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
                log.info(f"BOOST AUS: P01 {current_p01}→{NORMAL_DELTA}K, P02 {_p02_str}→{NORMAL_P02}K, P03 {current_p03}→{NORMAL_TEMP}°C (PCC={pcc:.2f} kW{_freq_str})")
                print(f"BOOST AUS: P01 {current_p01}→{NORMAL_DELTA}K | P02 {_p02_str}→{NORMAL_P02}K | P03 {current_p03}→{NORMAL_TEMP}°C | PCC={pcc:.2f} kW{_freq_str}")
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
