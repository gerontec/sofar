#!/usr/bin/python3
import os
import sys
import json
import time
import tinytuya
from db_config import get_db_connection

LOG_FILE   = '/tmp/hantech.log'
LOG_MAX_KB = 22

def trim_log():
    try:
        if os.path.getsize(LOG_FILE) <= LOG_MAX_KB * 1024:
            return
        with open(LOG_FILE, 'rb') as f:
            f.seek(-(LOG_MAX_KB * 1024), 2)
            data = f.read()
        # align to next newline
        nl = data.find(b'\n')
        data = data[nl + 1:] if nl != -1 else data
        with open(LOG_FILE, 'wb') as f:
            f.write(data)
    except OSError:
        pass

def get_parent():
    try:
        with open(f'/proc/{os.getppid()}/cmdline', 'rb') as f:
            return f.read().replace(b'\x00', b' ').decode().strip()
    except OSError:
        return '?'

DEVICE_ID      = 'bf83edca1b868db442fp00'
PLUG_DEVICE_ID = 'bf09fac39d369e977aroqh'   # Mess-Plug DachgeschossBalkon (misst die Klima)
DEVICES_JSON   = '/home/pi/python/devices.json'
LOCAL_TIMEOUT  = 5      # s — Socket-Timeout je lokalem Zugriff
LOCAL_RETRIES  = 2      # Versuche je lokalem Zugriff (LAN-Tuya ist gelegentlich zickig)
SET_SETTLE_S   = 2      # s — das Geraet uebernimmt den neuen Zustand verzoegert
SET_VERIFY_N   = 3      # Rueckleseversuche je Schaltversuch
PCC_ON_THRESH  = 18.0   # kW — einschalten ab
PCC_OFF_THRESH =  5.0   # kW — ausschalten unter (Hysterese)
AVG_MINUTES    = 2      # Mittelwert über 2 Minuten
STALE_MINUTES  = 5      # SOC-Wert darf max. 5 Minuten alt sein
DO4_WINDOW_MIN = 3      # min — do4-Peak (>20kW Abregelung) dieser Spanne zählt für Overflow-Trigger
# 10.10.2026 (user: air conditioning before DO4): switch on on the live PCC too (MQTT, every 4 s) and
# without the SOC lock — at >= 18 kW the export would be shed anyway (batmonitor 0.50-c fires DO4 only
# after 2 min > 20 kW, so this minute cron gets its chance). DO4 from batmonitor/do4_last (retained)
# instead of pv_relay_events (ESP era, no longer written). (the old SOC_MIN_BOOST 88 % lock is gone).
MQTT_BROKER    = '192.168.178.218'
INV_TOPIC      = 'inverter/power_grid_exchange/json'   # retained (pivot2db) + live every 4 s (sofar_fast)
DO4_LAST_TOPIC = 'batmonitor/do4_last'                 # retained {"ts", "pcc_w"} of the last DO4 pulse
MQTT_WAIT_S    = 6      # s — wait for a live inverter message / the retained DO4 time
INV_MAX_AGE_S  = 60     # s — a live PCC older than this does not count

# --- lokaler Zugriff auf die Klima (kein Cloud-Abo noetig) -------------------
# IP, Local Key und DP-Mapping kommen aus devices.json (schreibt `tinytuya wizard`).
# Der Local Key rotiert nur beim Neu-Anlernen des Geraets, nicht beim Ablauf des Abos.

_device   = None
_code2dp  = None   # {'switch': 1, ...}
_dp2code  = None   # {1: 'switch', ...}

def _load_device_entry():
    with open(DEVICES_JSON) as f:
        for entry in json.load(f):
            if entry.get('id') == DEVICE_ID:
                return entry
    raise KeyError(f'{DEVICE_ID} steht nicht in {DEVICES_JSON} — `python3 -m tinytuya wizard` laufen lassen')

def local_device():
    """tinytuya.Device fuer die Klima, einmal aufgebaut und gecacht."""
    global _device, _code2dp, _dp2code
    if _device is not None:
        return _device
    entry = _load_device_entry()
    ip  = entry.get('ip')
    key = entry.get('key')
    if not ip or not key:
        raise ValueError(f'IP oder Local Key fehlen in {DEVICES_JSON} — Wizard erneut laufen lassen')
    _dp2code = {int(dp): m['code'] for dp, m in (entry.get('mapping') or {}).items()}
    if not _dp2code:   # Fallback, falls der Wizard kein Mapping geliefert hat
        _dp2code = {1: 'switch', 2: 'temp_set', 3: 'temp_current', 4: 'mode',
                    18: 'humidity_current'}
    _code2dp = {code: dp for dp, code in _dp2code.items()}
    dev = tinytuya.Device(DEVICE_ID, ip, key)
    dev.set_version(float(entry.get('version') or 3.3))
    dev.set_socketTimeout(LOCAL_TIMEOUT)
    _device = dev
    return _device

def local_dps():
    """Status der Klima als {code: wert}; None wenn das Geraet im LAN nicht antwortet."""
    try:
        dev = local_device()
    except Exception as e:
        print(f"Lokaler Zugriff nicht moeglich: {e}")
        return None
    for _ in range(LOCAL_RETRIES):
        try:
            result = dev.status()
        except Exception as e:
            print(f"Lokale Statusabfrage fehlgeschlagen: {e}")
            continue
        if isinstance(result, dict) and 'dps' in result:
            return {_dp2code.get(int(dp), str(dp)): val for dp, val in result['dps'].items()}
        print(f"Lokale Statusabfrage ohne Daten: {result}")
    return None

_cloud = None

def get_cloud():
    """Cloud-Handle nur fuer den Mess-Plug — erst bei Bedarf aufbauen."""
    global _cloud
    if _cloud is None:
        _cloud = tinytuya.Cloud()
    return _cloud

def read_soc():
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
        print(f"DB-Fehler SOC: {e}")
        return None

def read_pcc():
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT AVG(ActivePower_PCC_Total), MAX(timestamp)
            FROM inverter_data
            WHERE ActivePower_PCC_Total IS NOT NULL
              AND timestamp >= NOW() - INTERVAL %s MINUTE
        """, (AVG_MINUTES,))
        row = cur.fetchone()
    conn.close()
    if not row or row[0] is None:
        return None, None
    return float(row[0]), str(row[1])

def read_plug_power():
    """Aktuelle Leistung des Mess-Plugs in W (via Cloud — der Plug haengt nicht im LAN des Pi);
    None falls der Plug nicht antwortet."""
    try:
        result = get_cloud().cloudrequest(f'/v1.0/devices/{PLUG_DEVICE_ID}/status')
        if not result.get('success'):
            return None
        for dp in result['result']:
            if dp['code'] == 'cur_power':
                return dp['value'] / 10.0
        return None
    except Exception as e:
        print(f"Plug-Leistung nicht lesbar: {e}")
        return None

def mqtt_get(topics, wait_s):
    """the newest payload (dict) per topic within wait_s; retained ones come at once"""
    import paho.mqtt.client as _mqtt
    got = {}
    def _on_msg(c, u, msg):
        try:
            d = json.loads(msg.payload.decode())
        except Exception:
            return
        if isinstance(d, dict):
            got[msg.topic] = d
    c = _mqtt.Client()
    c.on_message = _on_msg
    try:
        c.connect(MQTT_BROKER, 1883, 10)
        for t in topics:
            c.subscribe(t)
        c.loop_start()
        deadline = time.monotonic() + wait_s
        while len(got) < len(topics) and time.monotonic() < deadline:
            time.sleep(0.05)
        c.loop_stop()
        c.disconnect()
    except Exception as e:
        print(f"MQTT nicht erreichbar: {e}")
    return got


def read_live():
    """(live PCC kW or None, DO4 within DO4_WINDOW_MIN) from MQTT"""
    got = mqtt_get([INV_TOPIC, DO4_LAST_TOPIC], MQTT_WAIT_S)
    live = None
    inv = got.get(INV_TOPIC, {})
    v = inv.get('ActivePower_PCC_Total')
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        ts = inv.get('timestamp')
        age = None
        if isinstance(ts, (int, float)):
            age = time.time() - ts
        elif isinstance(ts, str):
            try:
                age = time.time() - time.mktime(time.strptime(ts[:19].replace('T', ' '), '%Y-%m-%d %H:%M:%S'))
            except ValueError:
                age = None
        if age is None or age <= INV_MAX_AGE_S:
            live = float(v)
    d4 = got.get(DO4_LAST_TOPIC, {}).get('ts')
    do4_recent = isinstance(d4, (int, float)) and time.time() - d4 <= DO4_WINDOW_MIN * 60
    return live, do4_recent

def set_switch(state):
    """Schaltet die Klima und PRUEFT den neuen Zustand durch Ruecklesen.

    Gibt nur True zurueck, wenn das Geraet den gewuenschten Zustand auch meldet.
    Die Quittung allein genuegt nicht: ein haengendes WLAN-Modul quittiert lokal
    wie ueber die Cloud jeden Befehl und fuehrt keinen aus (so geschehen in
    Bobingen, drei Tage lang unbemerkt).
    """
    try:
        dev = local_device()
    except Exception as e:
        print(f"Lokaler Zugriff nicht moeglich: {e}")
        return False
    dp   = _code2dp.get('switch', 1)
    want = bool(state)
    for _ in range(LOCAL_RETRIES):
        try:
            result = dev.set_value(dp, state)
        except Exception as e:
            print(f"Lokaler Schaltbefehl fehlgeschlagen: {e}")
            continue
        if isinstance(result, dict) and result.get('Error'):
            print(f"Lokaler Schaltbefehl abgelehnt: {result}")
            continue
        for _ in range(SET_VERIFY_N):
            time.sleep(SET_SETTLE_S)
            dps = local_dps()
            if dps is None or 'switch' not in dps:
                continue
            if bool(dps['switch']) == want:
                return True
        print(f"Quittiert, aber nicht ausgefuehrt: Geraet meldet weiter "
              f"{'AUS' if want else 'AN'} — Schaltbefehl wirkungslos")
    return False

def log_switch_event(action, state_new, reason, temp_c=None, humidity=None, power_w=None):
    duration_s = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if state_new == 0:
                cur.execute("""
                    SELECT ts FROM pv_relay_events
                    WHERE relay='hantech' AND state_new=1
                    ORDER BY ts DESC LIMIT 1
                """)
                row = cur.fetchone()
                if row:
                    from datetime import datetime
                    duration_s = int((datetime.now() - row[0]).total_seconds())
            cur.execute("""
                INSERT INTO pv_relay_events (ts, action, relay, duration_s, state_new, reason, temp_c, humidity, power_w)
                VALUES (NOW(), %s, 'hantech', %s, %s, %s, %s, %s, %s)
            """, (action, duration_s, state_new, reason, temp_c, humidity, power_w))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"DB-Logging fehlgeschlagen: {e}")

def print_status():
    dps = local_dps()
    if dps is None:
        print("Fehler: Klima antwortet lokal nicht")
        sys.exit(1)
    for code, val in dps.items():
        if code == 'switch':
            print(f"switch      : {'AN' if val else 'AUS'}")
        elif code == 'mode':
            print(f"Modus       : {val}")
        elif code == 'temp_set':
            print(f"Soll-Temp   : {val/10:.1f} °C")
        elif code == 'temp_current':
            print(f"Ist-Temp    : {val} °C")
        elif code == 'humidity_current':
            print(f"Luftfeuchte : {val} %")
    power_w = read_plug_power()
    print(f"Leistung    : {power_w:.1f} W" if power_w is not None else "Leistung    : n/a (Plug antwortet nicht)")

# --- main ---
trim_log()

if len(sys.argv) != 2:
    print("Verwendung: hantech.py on|off|status|auto")
    sys.exit(1)

cmd = sys.argv[1].lower()

if cmd == 'status':
    print_status()

elif cmd in ('on', 'off'):
    state = cmd == 'on'
    if set_switch(state):
        print(f"KlimaHantechDG: {'AN' if state else 'AUS'}")
    else:
        print("Fehler beim Schalten")
        sys.exit(1)

elif cmd == 'auto':
    pcc, ts = read_pcc()
    if pcc is None:
        print(f"KEIN PCC-Wert in DB (letzte {AVG_MINUTES} Min leer) — keine Aktion")
        sys.exit(0)

    soc = read_soc()

    # Ein lokaler Zugriff liefert alles: Schaltzustand, temp_set, Ist-Temp, Feuchte.
    dps = local_dps()
    if dps is None:
        print(f"KEIN Kontakt zur Klima (lokal, {DEVICE_ID}) — keine Aktion  caller={get_parent()}")
        sys.exit(0)

    is_on = dps.get('switch')

    # temp_set=210 (21°C) → manueller Dauerbetrieb, Auto-Abschaltung gesperrt
    temp_set      = dps.get('temp_set')
    temp_current  = dps.get('temp_current')
    hum_current   = dps.get('humidity_current')
    manual_override = (temp_set is not None and temp_set < 220)

    power_w = read_plug_power()

    ist_str = f"{temp_current}°C" if temp_current is not None else "?"
    pwr_str = f"{power_w:.0f}W" if power_w is not None else "n/a"
    print(f"PCC={pcc:.2f} kW ({AVG_MINUTES}min-avg)  ts={ts}  KlimaHantechDG={'AN' if is_on else 'AUS'}  SOC={soc}%  Ist={ist_str}  Plug={pwr_str}  temp_set={temp_set/10 if temp_set else '?'}°C  {'[MANUAL]' if manual_override else '[AUTO]'}  caller={get_parent()}")

    if temp_current is not None and temp_current > 30:
        print(f"⚠️  WARNUNG: Zimmertemperatur {temp_current}°C > 30°C")

    # Live PCC (4 s) and DO4 from batmonitor: catch the peak before the shedding.
    live_pcc, do4_recent = read_live() if not is_on and not manual_override else (None, False)
    peak = pcc >= PCC_ON_THRESH or (live_pcc is not None and live_pcc >= PCC_ON_THRESH)
    overflow = do4_recent and not peak
    if live_pcc is not None:
        print(f"PCC live={live_pcc:.2f} kW  do4_recent={do4_recent}")

    if manual_override:
        print("→ keine Änderung (MANUAL: temp_set=21°C, Auto-Abschaltung gesperrt)")
    elif not is_on and (peak or overflow):
        if set_switch(True):                 # no SOC lock: this export would be shed (10.10.2026)
            if overflow:
                print(f"→ EINGESCHALTET (do4 von batmonitor <{DO4_WINDOW_MIN} min, PCC={pcc:.2f} kW, SOC={soc}%)")
                log_switch_event('auto_on', 1, f"do4-overflow PCC={pcc:.2f}kW SOC={soc}% ts={ts}", temp_current, None, power_w)
            else:
                print(f"→ EINGESCHALTET (PCC={pcc:.2f} kW avg / live {live_pcc} kW >= {PCC_ON_THRESH} kW, SOC={soc}%)")
                log_switch_event('auto_on', 1, f"PCC={pcc:.2f}kW live={live_pcc} SOC={soc}% ts={ts}", temp_current, None, power_w)
        else:
            print("Fehler beim Einschalten")
    elif is_on and pcc < PCC_OFF_THRESH:
        if set_switch(False):
            print(f"→ AUSGESCHALTET (PCC={pcc:.2f} kW < {PCC_OFF_THRESH} kW)")
            log_switch_event('auto_off', 0, f"PCC={pcc:.2f}kW ts={ts}", temp_current, hum_current, power_w)
        else:
            print("Fehler beim Ausschalten")
    else:
        print("→ keine Änderung")

else:
    print("Verwendung: hantech.py on|off|status|auto")
    sys.exit(1)
