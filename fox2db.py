#!/usr/bin/env python3
import subprocess
import os
from datetime import datetime
import json
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import time
from typing import Tuple, Dict, Optional

# ═══════════════════════════════════════════════════════════════════════════
#                             KONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════
VERSION = "v1.50"

# State Power Mapping (Hardware)
STATE_TO_POWER = {0: 0, 1: 3000, 2: 3650, 3: 6650, 4: 3900, 5: 7100, 6: 7800, 7: 11400}
SORTED_STATES = sorted(STATE_TO_POWER.keys(), key=lambda s: STATE_TO_POWER[s])

# Regelwerk-Parameter
CONFIG = {
    'min_excess': 1010,
    'max_grid_draw': 1500,
    'max_soc': 99,
    'hysteresis': 505,
    'stabilization_cycles': 2,
    'emergency_import': 1020,
    'bat_discharge_threshold': -220,
    'sweet_spot_pcc': 160,
    'sweet_spot_bat': -310,
    'max_drop_rate': -20,
    'deep_discharge_lower': 6,              # Schutz aktiviert
    'deep_discharge_upper': 8,              # Schutz deaktiviert (UNTER Entladeschwelle!)
    'deep_discharge_charge_target': 7,      # Ziel-SOC während Notladung
}

# Dateipfade
PATHS = {
    'deep_discharge': '/run/user/1000/deep_discharge_protection_active.txt',
    'log': '/run/user/1000/fox2db.log',
    'relay_state': '/run/user/1000/current_relay_state.txt',
    'last_change': '/run/user/1000/last_relay_change.txt',
    'last_excess': '/run/user/1000/last_excess.txt',
    'ebox_data': '/run/user/1000/ebox15k.txt',
    'inverter_csv': '/run/user/1000/inverter.csv',
    'ebox_script': '/home/pi/python/ebox1arg.py',
    'ebyte_script': '/home/pi/python/ebyteserrequest.py',
}

# MQTT
MQTT_CONFIG = {
    'broker': 'kellertreppe.fritz.box',
    'port': 1883,
    'topic': 'inverter/power_grid_exchange/json',
    'timeout': 43
}

# ═══════════════════════════════════════════════════════════════════════════
#                          BLOCKING RULES MATRIX
# ═══════════════════════════════════════════════════════════════════════════
# Format: (name, check_function, applies_to)
# applies_to: 'up' = nur hochschalten, 'down' = nur runter, 'both' = beides

def create_blocking_rules(pcc, bat1, stable, drop_rate, pwr_diff):
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
        
        # v1.50: 'down' statt 'both' — Hochschalten wird nicht mehr durch Stabilisierung blockiert
        ('STABILIZING', 
         lambda: stable < CONFIG['stabilization_cycles'],
         'down'),
        
        ('HYSTERESIS', 
         lambda: pwr_diff < CONFIG['hysteresis'],
         'down'),
    ]

# ═══════════════════════════════════════════════════════════════════════════
#                            HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def log(msg: str):
    """Schreibt Log-Eintrag mit Timestamp"""
    try:
        with open(PATHS['log'], 'a') as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {VERSION}] {msg}\n")
    except Exception as e:
        print(f"Log Error: {e}")

def read_file_value(path: str, default=0, as_float=False) -> float:
    """Liest numerischen Wert aus Datei"""
    try:
        if os.path.exists(path):
            val = open(path).read().strip()
            if val:
                return (float if as_float else int)(val)
    except Exception as e:
        log(f"Error reading {path}: {e}")
    return default

def fetch_mqtt() -> Dict[str, float]:
    """Holt aktuelle Daten vom MQTT Broker - wirft Exception bei Timeout"""
    data = {'pcc': 0.0, 'bat1': 0.0, 'soc_bat1': 0.0}
    received = False
    
    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            log(f"MQTT Connected to {MQTT_CONFIG['broker']}")
            client.subscribe(MQTT_CONFIG['topic'])
        else:
            log(f"MQTT Connection failed: {reason_code}")
    
    def on_message(client, userdata, msg):
        nonlocal data, received
        try:
            payload = msg.payload.decode()
            j = json.loads(payload)
            
            data = {
                'pcc': float(j.get('ActivePower_PCC_Total') or 0) * 1000,
                'bat1': float(j.get('Power_Bat1') or 0) * 1000,
                'soc_bat1': float(j.get('SOC_Bat1') or 0)
            }
            
            log(f"MQTT Received: PCC={data['pcc']:.0f}W, Bat1={data['bat1']:.0f}W, SOC_Bat1={data['soc_bat1']:.1f}%")
            received = True
            client.disconnect()
            
        except json.JSONDecodeError as e:
            log(f"MQTT JSON Parse Error: {e}, Payload: {msg.payload}")
        except Exception as e:
            log(f"MQTT Message Error: {e}")
    
    try:
        client = mqtt.Client(CallbackAPIVersion.VERSION2)
        client.on_connect = on_connect
        client.on_message = on_message
        
        client.connect(MQTT_CONFIG['broker'], MQTT_CONFIG['port'], 60)
        client.loop_start()
        
        # Warte auf Nachricht
        timeout = MQTT_CONFIG['timeout']
        elapsed = 0
        while not received and elapsed < timeout:
            time.sleep(0.1)
            elapsed += 0.1
        
        client.loop_stop()
        client.disconnect()
        
        if not received:
            log(f"CRITICAL: MQTT Timeout after {timeout}s - NO DATA AVAILABLE")
            raise RuntimeError("MQTT timeout - aborting cycle for safety")
        
    except Exception as e:
        log(f"MQTT Error: {e}")
        raise  # Re-raise um main() zu stoppen
    
    return data

def read_ebox() -> Tuple[float, float]:
    """Liest EBox-Daten: (Strom in A, minimaler SOC in %)"""
    try:
        if not os.path.exists(PATHS['ebox_data']):
            log(f"EBox data file not found: {PATHS['ebox_data']}")
            return 0.0, 0.0
            
        lines = open(PATHS['ebox_data']).readlines()
        socs, current = [], 0.0
        
        for l in lines:
            l = l.replace("b'", "").replace("'", "").strip()
            if 'Power Volt' in l:
                continue
            if l and len(l) > 0 and l[0] in '123':
                flds = l.split()
                if len(flds) > 2:
                    try:
                        current += float(flds[2])
                    except ValueError:
                        pass
                for f in flds:
                    if '%' in f:
                        try:
                            socs.append(float(f.replace('%', '')))
                        except ValueError:
                            pass
        
        total_current = current / 1000.0  # mA -> A
        min_soc = min(socs) if socs else 0.0
        
        return total_current, min_soc
        
    except Exception as e:
        log(f"Error reading EBox data: {e}")
        return 0.0, 0.0

# ═══════════════════════════════════════════════════════════════════════════
#                          FB STATE CONTROLLER
# ═══════════════════════════════════════════════════════════════════════════

def fb_controller(soc, pcc, bat_cur, bat1, relay_st, prot) -> Tuple[int, str, float]:
    """
    Fuzzy-Logic basierter State Controller mit verbesserter Deep-Discharge-Hysterese
    
    Returns:
        (best_state, trace_message, calculated_excess)
    """
    # Berechne echten Überschuss
    ebox_eff = max(bat_cur * 2 * 53 + 1, STATE_TO_POWER.get(relay_st, 0)) if relay_st > 0 else 0
    excess = pcc + ebox_eff + bat1
    
    # NOTLADUNG mit Zielvorgabe - verhindert Ping-Pong
    if prot:
        if soc < CONFIG['deep_discharge_charge_target']:
            return 1, f"EMERGENCY_CHARGE_TO_{CONFIG['deep_discharge_charge_target']}% (aktuell {soc:.1f}%)", excess
        else:
            return 0, f"CHARGE_TARGET_REACHED ({soc:.1f}% >= {CONFIG['deep_discharge_charge_target']}%)", excess
    
    # Kritischer SOC ohne aktiven Schutz → aktiviere Schutz
    if soc < CONFIG['deep_discharge_lower']:
        return 1, f"CRITICAL_SOC_PROTECTION_ACTIVATE ({soc:.1f}% < {CONFIG['deep_discharge_lower']}%)", excess
    
    if soc >= CONFIG['max_soc']:
        return 0, "BATTERY_FULL_STOP", excess
    
    if excess < CONFIG['min_excess']:
        return 0, f"INSUFFICIENT_EXCESS ({excess:.0f}W)", excess
    
    # Power Matching
    budget = excess + CONFIG['max_grid_draw']
    best = max((s for s, p in STATE_TO_POWER.items() if p <= budget), 
               key=lambda s: STATE_TO_POWER[s], default=0)
    
    trace = f"POWER_MATCHING (Excess: {excess:.0f}W, Budget: {budget:.0f}W)"
    
    # Rampen-Limitierung
    if best > relay_st:
        idx = SORTED_STATES.index(relay_st) if relay_st in SORTED_STATES else 0
        next_st = SORTED_STATES[min(idx + 1, len(SORTED_STATES) - 1)]
        if best > next_st:
            trace += f" | RAMP_LIMITED ({best}->{next_st})"
            best = next_st
    
    return best, trace, excess

# ═══════════════════════════════════════════════════════════════════════════
#                              MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════

def main():
    log("--- Start Cycle ---")
    
    # INPUT LAYER
    try:
        # 1. Update EBox-Daten
        try:
            subprocess.run(
                f"{PATHS['ebox_script']} pwr 1 > {PATHS['ebox_data']}", 
                shell=True, 
                check=True,
                timeout=10
            )
        except subprocess.TimeoutExpired:
            log("EBox Update timeout")
        except Exception as e:
            log(f"EBox Update failed: {e}")
        
        # 2. Daten einlesen - MQTT MUSS funktionieren!
        mqtt_data = fetch_mqtt()
        
    except Exception as e:
        # MQTT-Fehler → Notabschaltung
        log(f"EMERGENCY SHUTDOWN: {e}")
        try:
            subprocess.run([PATHS['ebyte_script'], '0'], check=True, timeout=10)
            open(PATHS['relay_state'], 'w').write('0')
            log("→ Forced State 0 (safe mode)")
        except Exception as relay_err:
            log(f"Emergency relay shutdown failed: {relay_err}")
        return  # Script beenden OHNE weitere Berechnungen
    
    # Ab hier nur wenn MQTT funktioniert hat
    bat_cur, soc = read_ebox()
    pcc = mqtt_data.get('pcc', 0)
    bat1 = mqtt_data.get('bat1', 0)
    soc_bat1 = mqtt_data.get('soc_bat1', 0)
    
    relay_st = read_file_value(PATHS['relay_state'])
    stable = read_file_value(PATHS['last_change'])
    prot = read_file_value(PATHS['deep_discharge']) == 1
    last_excess = read_file_value(PATHS['last_excess'], as_float=True)
    
    # LOGIC LAYER
    best, trace, excess = fb_controller(soc, pcc, bat_cur, bat1, relay_st, prot)
    
    # Berechne Trend
    drop_rate = (excess - last_excess) / 30.0 if last_excess > 0 else 0
    open(PATHS['last_excess'], 'w').write(f"{excess:.1f}")
    
    final = relay_st
    changed = False
    
    if best != relay_st:
        pwr_diff = abs(STATE_TO_POWER[best] - STATE_TO_POWER[relay_st])
        direction = 'up' if best > relay_st else 'down'
        emergency = pcc < -CONFIG['emergency_import'] and direction == 'down'
        
        if emergency:
            log(f"EMERGENCY: Force Shutdown {relay_st}->{best} (Import={pcc:.0f}W!)")
            final, changed = best, True
        else:
            # Prüfe Blocking Rules Matrix
            rules = create_blocking_rules(pcc, bat1, stable, drop_rate, pwr_diff)
            blocked = False
            
            for name, check, applies in rules:
                if applies in [direction, 'both']:
                    if check():
                        if name == 'TREND_BLOCK':
                            trace += f" | {name} (Drop: {drop_rate:.1f}W/s)"
                        elif name == 'SWEET_SPOT_HOLD':
                            trace += f" | {name} (PCC={pcc:.0f}W, Bat1={bat1:.0f}W)"
                        elif name == 'BAT_GUARD_BLOCK':
                            trace += f" | {name} (Bat1={bat1:.0f}W)"
                        elif name == 'HYSTERESIS':
                            trace += f" | BLOCKED: {name} ({pwr_diff}W)"
                        else:
                            trace += f" | BLOCKED: {name}"
                        blocked = True
                        break
            
            if not blocked:
                final, changed = best, True
    
    # OUTPUT LAYER
    new_stable = 0 if changed else stable + 1
    ebox_w = bat_cur * 2 * 53
    
    log(f"Data: SOC2={soc:.1f}% SOC1={soc_bat1:.1f}% PCC={pcc:.0f}W Bat1={bat1:.0f}W EBox={ebox_w:.0f}W (State={relay_st}) Stable={new_stable}")
    if last_excess > 0 and drop_rate != 0:
        log(f"Trend: Excess {last_excess:.0f}W→{excess:.0f}W ({drop_rate:+.1f}W/s)")
    log(f"Result: State {final} (TRACE: {trace})")
    
    # MQTT-Publish
    payload = {
        "ts": datetime.now().isoformat(timespec='seconds'),
        "version": VERSION,
        "soc_bat2": round(float(soc), 1),
        "soc_bat1": round(float(soc_bat1), 1),
        "pcc": round(float(pcc)),
        "bat1": round(float(bat1)),
        "ebox": round(float(ebox_w)),
        "state": final,
        "state_before": relay_st,
        "stable": new_stable,
        "excess": round(float(excess)),
        "drop_rate": round(drop_rate, 1) if last_excess > 0 and drop_rate != 0 else None,
        "trace": trace,
        "deep_discharge_active": prot,
    }
    
    json_str = json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
    cmd = ["python3", "/home/pi/python/fox2mqtt.py", json_str]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=5)
    except subprocess.TimeoutExpired:
        log("MQTT-Script timeout")
    except subprocess.CalledProcessError as e:
        log(f"MQTT-Script Error: {e.stderr}")
    except Exception as e:
        log(f"MQTT-Execution failed: {e}")
    
    # Dateien schreiben
    open(PATHS['last_change'], 'w').write(str(new_stable))
    
    # Deep Discharge Protection mit verbesserter Hysterese
    old_prot = prot
    if soc < CONFIG['deep_discharge_lower']:
        if not old_prot:  # Nur loggen wenn neu aktiviert
            log(f"⚠️  DEEP_DISCHARGE_PROTECTION ACTIVATED at {soc:.1f}% (threshold: {CONFIG['deep_discharge_lower']}%)")
        open(PATHS['deep_discharge'], 'w').write("1")
    elif soc >= CONFIG['deep_discharge_upper']:
        if old_prot:  # Nur loggen wenn deaktiviert
            log(f"✓ DEEP_DISCHARGE_PROTECTION DEACTIVATED at {soc:.1f}% (threshold: {CONFIG['deep_discharge_upper']}%)")
        open(PATHS['deep_discharge'], 'w').write("0")
    # Zwischenbereich: Status bleibt unverändert (Hysterese)
    
    # Inverter CSV schreiben (für soyo1min.py)
    try:
        open(PATHS['inverter_csv'], 'w').write(f"{int(pcc)},{int(bat1)},{soc:.1f},{soc_bat1:.1f},{bat_cur:.1f},{final}\n")
    except Exception as e:
        log(f"Error writing inverter.csv: {e}")
    
    # ═══════════════════════════════════════════════════════════════════════
    # RELAY STATE AKTUALISIEREN — v1.50: verbesserte Fehlerbehandlung
    # relay_state wird NUR geschrieben wenn der Relay-Befehl erfolgreich war
    # stdout/stderr werden bei Fehler im Log festgehalten
    # ═══════════════════════════════════════════════════════════════════════
    try:
        if changed:
            result = subprocess.run(
                [PATHS['ebyte_script'], str(final)],
                check=True,
                capture_output=True,
                text=True,
                timeout=10
            )
            # Nur bei Erfolg: State-Datei aktualisieren
            open(PATHS['relay_state'], 'w').write(str(final))
            log(f"Relay OK: {relay_st}->{final} | {result.stdout.strip()}")
        else:
            # Keine Änderung, State-Datei trotzdem aktuell halten
            open(PATHS['relay_state'], 'w').write(str(final))
    except subprocess.TimeoutExpired:
        log(f"Relay TIMEOUT: Versuch State {final} — State-Datei NICHT aktualisiert")
    except subprocess.CalledProcessError as e:
        log(f"Relay FEHLER State {final}: stdout=[{e.stdout.strip()}] stderr=[{e.stderr.strip()}] — State-Datei NICHT aktualisiert")
    except Exception as e:
        log(f"Relay FEHLER State {final}: {type(e).__name__}: {e} — State-Datei NICHT aktualisiert")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Script terminated by user")
    except Exception as e:
        log(f"Fatal error in main: {e}")
        import traceback
        log(traceback.format_exc())
