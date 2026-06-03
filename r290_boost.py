#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Cron: jede Minute. Bei >8 kW Einspeisung: P01+P02 (Hysterese) → 1K, P03 (WW-Soll) → 55°C.
# Bei <3 kW Einspeisung: P01+P02 → 10K/3K, P03 → 48°C.
# PCC-Vorzeichen: positiv = Einspeisung ins Netz.

VERSION = "1.5"

from pymodbus.client import ModbusSerialClient
from db_config import get_db_connection
import sys
import os
import time
import signal
import logging
import json
from logging.handlers import RotatingFileHandler

SCRIPT_TIMEOUT = 20  # Sekunden (15s + 30%)

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
BOOST_DELTA       = 1               # K — P01+P02 Einschalthysterese bei Überschuss
NORMAL_DELTA      = 10              # K — P01 Einschalthysterese normal
NORMAL_P02        = 6               # K — P02 Normalwert (aus Backup bestätigt: 6K)
BOOST_TEMP        = 55              # °C — WW-Solltemperatur bei Überschuss
NORMAL_TEMP       = 48              # °C — WW-Solltemperatur normal (kein Überschuss)
EXCESS_THRESHOLD    = 18.0   # kW — Boost AN (Peak-Begrenzung)
BOOST_OFF_THRESHOLD = 4      # kW — Boost AUS wenn PCC < 4 kW
STALE_MINUTES       = 5      # PCC-Wert darf max. 5 Minuten alt sein
SMOOTH_MINUTES      = 2      # Glättung: Durchschnitt über letzte N Minuten
SOC_MIN_BOOST       = 88     # % — Boost nur starten wenn Akku-SOC > diesem Wert

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
            """, (max(STALE_MINUTES, SMOOTH_MINUTES),))
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

def write_reg(client, reg, value, label, retries=2):
    for attempt in range(retries + 1):
        w = client.write_register(address=reg, value=value, slave=SLAVE_ID)
        if not w.isError():
            return True
        if attempt < retries:
            time.sleep(0.4)
    log.error(f"Schreiben {label}={value} fehlgeschlagen: {w}")
    return False

def write_regs_bulk(client, start_reg, values, label, retries=2):
    """FC16 Write Multiple Registers. Fällt bei Fehler auf Einzelschreibung zurück."""
    for attempt in range(retries + 1):
        w = client.write_registers(address=start_reg, values=values, slave=SLAVE_ID)
        if not w.isError():
            log.info(f"FC16 OK: {label} @ 0x{start_reg:04X} = {values}")
            return True
        if attempt < retries:
            time.sleep(0.4)
    log.warning(f"FC16 fehlgeschlagen ({label}): {w} — Fallback auf Einzelschreibung")
    ok = True
    for i, val in enumerate(values):
        ok &= write_reg(client, start_reg + i, val, f"{label}[{i}]")
    return ok

MQTT_BROKER    = '192.168.178.218'
MQTT_FOX_TOPIC = 'fox2db/state'

def read_mqtt_trigger():
    """Liest fox2db/state (retained) — gibt need_downward_regulation zurück."""
    import paho.mqtt.client as _mqtt
    result = []
    def _on_msg(c, u, msg):
        try:
            result.append(bool(json.loads(msg.payload.decode()).get('need_downward_regulation', False)))
        except Exception:
            pass
        c.disconnect()
    c = _mqtt.Client()
    c.on_message = _on_msg
    try:
        c.connect(MQTT_BROKER, 1883, 10)
        c.subscribe(MQTT_FOX_TOPIC)
        c.loop_start()
        deadline = time.monotonic() + 3.0
        while not result and time.monotonic() < deadline:
            time.sleep(0.05)
        c.loop_stop()
        c.disconnect()
    except Exception as e:
        log.warning(f'MQTT fox2db/state nicht erreichbar: {e}')
    val = result[0] if result else False
    log.info(f'MQTT need_downward_regulation={val}')
    return val


def open_modbus():
    c = ModbusSerialClient(method='rtu', port=PORT, baudrate=BAUDRATE, timeout=3)
    if not c.connect():
        log.error("Modbus Verbindung fehlgeschlagen")
        return None
    return c

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

mqtt_trigger = read_mqtt_trigger()
trigger_on   = (pcc >= ACTIVE_THRESHOLD) or mqtt_trigger
trigger_src  = 'MQTT:need_downward_regulation' if mqtt_trigger else f'PCC={pcc:.2f} kW'

log.info(f"Schwelle: {ACTIVE_THRESHOLD} kW (Peak-Begrenzung) | mqtt_trigger={mqtt_trigger}")

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
        log.info(f"Normal: P01={current_p01}K, P02={_p02_str}, P03={current_p03}°C, PCC={pcc:.2f} kW{_freq_str}")
else:
    # Hysterese-Bereich: keine Änderung
    if boost_is_active:
        log.info(f"BOOST Hysterese: P01={current_p01}K, P02={_p02_str}, P03={current_p03}°C, PCC={pcc:.2f} kW{_freq_str}")
    else:
        log.info(f"Normal: P01={current_p01}K, P02={_p02_str}, P03={current_p03}°C, PCC={pcc:.2f} kW{_freq_str}")
