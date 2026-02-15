# Testing Guide - Sofar C Programs

## 📋 Auf dem Raspberry Pi installieren

```bash
# 1. Code holen
cd ~/sofar
git fetch origin claude/fix-temp-file-paths-fesdi
git reset --hard origin/claude/fix-temp-file-paths-fesdi

# 2. Dependencies prüfen
sudo apt-get update
sudo apt-get install -y libpaho-mqtt3c-dev libcjson-dev libmodbus-dev gcc make

# 3. Kompilieren
make clean
make

# 4. Installieren
sudo make install

# 5. Verifizieren
fox2db --version
ebox --version
fox2mqtt --version
ebyte --version
```

---

## 🧪 Test 1: Einzelne Programme testen

### ebyte (Relay Control)

```bash
# Status anzeigen
sudo ebyte --status

# Relay setzen
sudo ebyte 0      # Alle OFF
sudo ebyte 7      # Alle ON

# Soyo Kommando
sudo ebyte --soyo 200
```

**Erwartete Ausgabe:**
```
EbyteRTU-C: Request=7 → Relaisbits=[1,1,1]
EbyteRTU-C: SUCCESS → Relays set to state 7 [1,1,1]
EbyteRTU-C: Operation took 1234 µs (1.23 ms)
```

### fox2mqtt (MQTT Publisher)

```bash
# Test-JSON senden
fox2mqtt '{"test":123,"status":"ok"}'
```

**Erwartete Ausgabe:**
```
→ MQTT published (25 bytes) → fox2db/state
```

### ebox (Battery Communication)

```bash
# Simple mode
ebox bat

# Parse mode (schreibt in DB)
ebox bat --parse
```

**Erwartete Ausgabe:**
```
bat
# ... Batterie-Daten ...
```

---

## 🚀 Test 2: fox2db Integration Test

```bash
cd ~/sofar
./test_fox2db.sh
```

**Was wird getestet:**
- ✓ Alle C-Programme sind installiert
- ✓ fox2db startet und verbindet sich mit MQTT
- ✓ Empfängt Inverter-Daten
- ✓ Ruft `ebyte` für Relay-Steuerung auf
- ✓ Ruft `fox2mqtt` für Status-Publishing auf
- ✓ Läuft ohne Segfaults durch

**Erwartete Ausgabe:**
```
==========================================
  fox2db C Integration Test
==========================================

Checking installed programs...
  ✓ fox2db: fox2db v1.50-C
  ✓ ebox: ebox v1.0.0
  ✓ fox2mqtt: fox2mqtt v1.0.0
  ✓ ebyte: ebyte v1.0.0

Configuration:
  EBox Script:         /home/pi/python/ebox1arg.py
  EByte Script:        /usr/local/bin/ebyte
  MQTT Publish Script: /usr/local/bin/fox2mqtt

Starting fox2db...
==========================================

[2026-02-15 20:00:00 v1.50-C] === fox2db v1.50-C started ===
[2026-02-15 20:00:00 v1.50-C] MQTT Broker: 192.168.178.218:1883
[2026-02-15 20:00:02 v1.50-C] MQTT Connected
[2026-02-15 20:00:12 v1.50-C] MQTT Received: PCC=-4330W, Bat1=0W, SOC_Bat1=10.0%
[2026-02-15 20:00:12 v1.50-C] === fox2db finished ===

real    0m12.787s
user    0m1.135s
sys     0m0.245s

==========================================
Test completed with exit code: 0
==========================================
```

---

## 🔍 Debugging

### Wenn fox2db crasht:

```bash
# Mit Debug-Symbolen kompilieren
gcc -g -O0 -std=c11 -o fox2db fox2db.c -lpaho-mqtt3c -lcjson -lm

# Mit GDB starten
gdb ./fox2db

# In GDB:
(gdb) run --mqtt-broker "192.168.178.218"
# Warten auf Crash...
(gdb) backtrace
(gdb) quit
```

### Logs prüfen:

```bash
# fox2db Log
tail -f /run/user/1000/fox2db.log

# Systemd Logs (wenn als Service)
journalctl -u fox2db -f
```

### Relay-Status prüfen:

```bash
# Aktueller State
cat /run/user/1000/current_relay_state.txt

# Letzter Wechsel
cat /run/user/1000/last_relay_change.txt

# EBox Daten
cat /run/user/1000/ebox15k.txt
```

---

## ⚡ Performance-Vergleich

### Python vs C:

```bash
# Python (alt)
time python3 /home/pi/python/ebyteserrequest.py 7

# C (neu)
time sudo ebyte 7
```

**Erwartete Speedups:**
- Relay Control: **~50x schneller**
- MQTT Publish: **~30x schneller**
- Startup: **~100x schneller**
- Memory: **~200x weniger**

---

## 📝 Integration in bestehende my_config.sh

Wenn du bereits `my_config.sh` hast, ändere nur diese Zeilen:

```bash
# ALT (Python):
--ebyte-script "/home/pi/python/ebyteserrequest.py" \
--mqtt-publish-script "/home/pi/python/fox2mqtt.py"

# NEU (C):
--ebyte-script "/usr/local/bin/ebyte" \
--mqtt-publish-script "/usr/local/bin/fox2mqtt"
```

---

## 🎯 Erfolgs-Kriterien

✅ **Test erfolgreich wenn:**
1. Alle Programme installiert (`--version` funktioniert)
2. `ebyte --status` zeigt Relay-Status
3. `fox2mqtt '{"test":1}'` sendet ohne Fehler
4. `./test_fox2db.sh` läuft durch (exit code 0)
5. Keine Segfaults im Log
6. Relays werden korrekt geschaltet

❌ **Probleme beheben:**
- **"command not found"** → `sudo make install` ausführen
- **"Permission denied"** → `sudo` verwenden bei ebyte
- **Segfault** → siehe Debugging-Abschnitt
- **MQTT timeout** → Broker-Adresse prüfen

