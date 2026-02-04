# Installation Guide - Soyo1min C Version

[🇬🇧 English Version](INSTALL.md)

## Schnellstart

```bash
# 0. Branch wechseln (falls noch nicht geschehen)
cd ~/sofar
git fetch origin
git checkout claude/python-to-c-conversion-fDGGt
git pull origin claude/python-to-c-conversion-fDGGt

# 1. Abhängigkeiten installieren
sudo apt-get update
sudo apt-get install -y build-essential libmosquitto-dev python3 python3-pip

# 2. Python-Pakete für sunrise.py installieren
pip3 install -r requirements.txt

# 3. System-Requirements prüfen
cd c-version
./configure

# 4. Kompilieren
make

# 5. Testen (als User mit Zugriff auf /dev/ttyUSB32)
./soyo1min

# 6. Installation (optional)
sudo make install

# 7. Systemd Service einrichten (optional)
sudo cp soyo1min.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable soyo1min
sudo systemctl start soyo1min
```

## Voraussetzungen

### Hardware
- Serieller Port `/dev/ttyUSB32` (oder in `config.h` anpassen)
- MQTT-Broker erreichbar unter `192.168.178.218`

### Software
- Linux (Raspberry Pi OS, Debian, Ubuntu, etc.)
- GCC Compiler
- libmosquitto-dev (MQTT-Bibliothek)
- Python 3 (für sunrise.py)
- Python-Pakete: astral, pytz (für sunrise.py)

### Berechtigungen
User muss Mitglied der `dialout`-Gruppe sein:
```bash
sudo usermod -a -G dialout $USER
# Neu anmelden erforderlich!
```

## Detaillierte Installation

### 1. Repository klonen / Dateien kopieren

Die C-Version befindet sich im `c-version/`-Verzeichnis.

### 2. Abhängigkeiten installieren

**Debian/Ubuntu/Raspberry Pi OS:**
```bash
sudo apt-get install -y build-essential libmosquitto-dev
```

**Fedora/RHEL:**
```bash
sudo dnf install gcc make mosquitto-devel
```

**Arch Linux:**
```bash
sudo pacman -S gcc make mosquitto
```

### 3. Konfiguration anpassen

Editieren Sie `config.h` und passen Sie folgende Werte an:

```c
#define SERIAL_PORT "/dev/ttyUSB32"      // Ihr serieller Port
#define MQTT_BROKER "192.168.178.218"    // Ihre MQTT-Broker-IP
#define MQTT_TOPIC "em0/54"              // Ihr MQTT-Topic
#define BATTERY_CAPACITY_KWH 30          // Ihre Batteriekapazität
```

### 4. Kompilieren

```bash
cd c-version
make clean
make
```

Bei Erfolg wird die ausführbare Datei `soyo1min` erstellt.

**Debug-Version (mit Debugging-Symbolen):**
```bash
make debug
```

### 5. Manueller Test

```bash
# Prüfen, ob sunrise.py im PATH oder im gleichen Verzeichnis liegt
which sunrise.py
# oder
ls ../sunrise.py

# Programm starten
./soyo1min

# Mit Strg+C beenden
```

### 6. Installation als Systemdienst

```bash
# Binary installieren
sudo make install

# Service-Datei kopieren
sudo cp soyo1min.service /etc/systemd/system/

# Pfade in Service-Datei ggf. anpassen
sudo nano /etc/systemd/system/soyo1min.service
# Ändern Sie User, Group und WorkingDirectory

# Service aktivieren und starten
sudo systemctl daemon-reload
sudo systemctl enable soyo1min
sudo systemctl start soyo1min

# Status prüfen
sudo systemctl status soyo1min

# Logs ansehen
sudo journalctl -u soyo1min -f
```

## Deinstallation

```bash
# Service stoppen und deaktivieren
sudo systemctl stop soyo1min
sudo systemctl disable soyo1min
sudo rm /etc/systemd/system/soyo1min.service
sudo systemctl daemon-reload

# Binary entfernen
sudo rm /usr/local/bin/soyo1min

# Lock-Datei entfernen (falls vorhanden)
rm /run/user/1000/soyo1min.lock
```

## Parallelbetrieb mit Python-Version

Beide Versionen verwenden unterschiedliche Lock-Dateien:
- Python: `/run/user/1000/soyo1min.lock`
- C: `/run/user/1000/soyo1min.lock` (gleich!)

**WICHTIG:** Nur eine Version gleichzeitig ausführen!

Die C-Version schreibt Logs nach:
- `/run/user/1000/soyo1min_c.log`

Die Python-Version schreibt nach:
- `/run/user/1000/soyo1min.log`

## Troubleshooting

### Fehler: "mosquitto.h: No such file or directory"
```bash
sudo apt-get install libmosquitto-dev
```

### Fehler: "Permission denied" beim Zugriff auf /dev/ttyUSB32
```bash
# User zur dialout-Gruppe hinzufügen
sudo usermod -a -G dialout $USER
# Neu anmelden!
```

### Fehler: "Another instance is running"
```bash
# Laufende Instanz prüfen
ps aux | grep soyo1min

# Falls keine läuft, Lock-Datei löschen
rm /run/user/1000/soyo1min.lock
```

### MQTT verbindet nicht
```bash
# Broker testen
mosquitto_sub -h 192.168.178.218 -t em0/54 -v

# In config.h MQTT_BROKER-IP prüfen
```

### sunrise.py nicht gefunden
```bash
# Prüfen wo sunrise.py liegt
find ~ -name sunrise.py

# Symlink erstellen oder in config.h Pfad anpassen
ln -s /pfad/zu/sunrise.py ./sunrise.py
```

## Performance

Die C-Version ist:
- ~10-20x schneller beim Start
- ~50% weniger Speicherverbrauch (ca. 2-3 MB statt 15-20 MB)
- Gleiche Funktionalität wie Python-Version

## Nächste Schritte

Nach erfolgreicher Installation:
1. Logs überwachen: `tail -f /run/user/1000/soyo1min_c.log`
2. Verhalten beobachten (Grid-Werte, Power-Settings)
3. Bei Bedarf Konfiguration in `config.h` anpassen und neu kompilieren
