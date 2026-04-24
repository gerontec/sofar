#!/usr/bin/python3
"""
ebyte_ctrl.py — Zentrales Steuer-Script Ebyte MA01-XACX0440 (4 DO)

Verwendung:
  ebyte_ctrl.py STATE            → Relais 1-3 auf State 0-7 setzen (fox2db)
  ebyte_ctrl.py --state          → aktuellen State (0-7) ausgeben
  ebyte_ctrl.py r4 on            → Relais 4 einschalten
  ebyte_ctrl.py r4 off           → Relais 4 ausschalten
  ebyte_ctrl.py r4 pulse [SEK]   → Relais 4 für SEK Sekunden Puls (default 3)
  ebyte_ctrl.py all off          → alle 4 Relais ausschalten
"""

import sys
import os
import time
from pymodbus.client.sync import ModbusSerialClient

RTU_PORT    = "/dev/ttyAMA0"
BAUDRATE    = 9600
SLAVE_ID    = 1
COIL_START  = 0          # Coil 0=R1, 1=R2, 2=R3, 3=R4

LOCKFILE_R4 = "/tmp/ebyte_r4_pulse.lock"
STATE_FILE  = "/run/user/1000/current_relay_state.txt"

STATE_TO_BITS = {
    0: [0,0,0], 1: [0,0,1], 2: [0,1,0], 3: [0,1,1],
    4: [1,0,0], 5: [1,0,1], 6: [1,1,0], 7: [1,1,1], 11: [1,1,1]
}
BITS_TO_STATE = {
    (0,0,0):0, (0,0,1):1, (0,1,0):2, (0,1,1):3,
    (1,0,0):4, (1,0,1):5, (1,1,0):6, (1,1,1):7
}

def log(msg):
    print(f"EbyteCtrl: {msg}", flush=True)

def make_client():
    return ModbusSerialClient(
        method="rtu", port=RTU_PORT, baudrate=BAUDRATE,
        parity="N", stopbits=1, bytesize=8, timeout=2, retries=1
    )

# ── Relais 1-3 State ──────────────────────────────────────────────────────────

def cmd_set_state(state):
    if state not in STATE_TO_BITS:
        log(f"FEHLER: Ungültiger State {state}. Gültig: {sorted(STATE_TO_BITS.keys())}")
        sys.exit(1)
    if not os.path.exists(RTU_PORT):
        log(f"FEHLER: {RTU_PORT} nicht vorhanden")
        sys.exit(1)
    bits = STATE_TO_BITS[state]
    log(f"State {state} → Bits {bits}")
    c = make_client()
    if not c.connect():
        log("Verbindung fehlgeschlagen")
        sys.exit(1)
    r = c.write_coils(COIL_START, bits, unit=SLAVE_ID)
    c.close()
    if r and not r.isError():
        log(f"ERFOLG State {state}")
    else:
        log(f"FEHLER beim Schreiben: {r}")
        sys.exit(1)

def cmd_get_state():
    c = make_client()
    if not c.connect():
        if os.path.exists(STATE_FILE):
            print(open(STATE_FILE).read().strip())
            sys.exit(0)
        print("-1", file=sys.stderr)
        sys.exit(1)
    r = c.read_coils(COIL_START, 3, unit=SLAVE_ID)
    c.close()
    if r and not r.isError():
        bits = tuple(1 if b else 0 for b in r.bits[:3])
        print(BITS_TO_STATE.get(bits, -1))
    else:
        print("-1", file=sys.stderr)
        sys.exit(1)

# ── Relais 4 ─────────────────────────────────────────────────────────────────

def _r4_write(on: bool):
    c = make_client()
    if not c.connect():
        log("Verbindung fehlgeschlagen")
        sys.exit(1)
    r = c.write_coil(COIL_START + 3, on, unit=SLAVE_ID)
    c.close()
    if r and not r.isError():
        log(f"Relais 4 {'EIN' if on else 'AUS'}")
    else:
        log(f"FEHLER: {r}")
        sys.exit(1)

def cmd_r4_pulse(seconds: int):
    # Exklusives Lockfile — kein paralleler Puls
    try:
        fd = os.open(LOCKFILE_R4, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
    except FileExistsError:
        try:
            pid = int(open(LOCKFILE_R4).read().strip())
            os.kill(pid, 0)          # Wirft OSError wenn Prozess tot
            log(f"Puls läuft bereits (PID {pid}) — überspringe")
            sys.exit(0)
        except (ValueError, ProcessLookupError, OSError):
            os.unlink(LOCKFILE_R4)   # Veraltetes Lockfile
            cmd_r4_pulse(seconds)
            return
    try:
        _r4_write(True)
        time.sleep(seconds)
        _r4_write(False)
    finally:
        try:
            os.unlink(LOCKFILE_R4)
        except OSError:
            pass

# ── Alle aus ─────────────────────────────────────────────────────────────────

def cmd_all_off():
    c = make_client()
    if not c.connect():
        log("Verbindung fehlgeschlagen")
        sys.exit(1)
    for coil in range(4):
        c.write_coil(COIL_START + coil, False, unit=SLAVE_ID)
    c.close()
    log("Alle 4 Relais AUS")

# ── Argument-Dispatch ─────────────────────────────────────────────────────────

args = sys.argv[1:]

if not args:
    print(__doc__)
    sys.exit(0)

if args[0] == "--state":
    cmd_get_state()

elif args[0] == "r4":
    sub = args[1] if len(args) > 1 else ""
    if sub == "on":
        _r4_write(True)
    elif sub == "off":
        _r4_write(False)
    elif sub == "pulse":
        sek = int(args[2]) if len(args) > 2 else 3
        cmd_r4_pulse(sek)
    else:
        log(f"Unbekannt: r4 {sub}. Gültig: on, off, pulse [SEK]")
        sys.exit(1)

elif args[0] == "all" and len(args) > 1 and args[1] == "off":
    cmd_all_off()

else:
    try:
        cmd_set_state(int(args[0]))
    except ValueError:
        log(f"Unbekannter Befehl: {args[0]}")
        sys.exit(1)
