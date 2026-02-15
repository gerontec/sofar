# Soyo1min - C Version

[🇬🇧 English Version](README.md)

C-Implementierungen für Sofar Inverter Management.

## Programme

### soyo1min
Batterie-Management-System - verwaltet die Batterieentladung basierend auf:
- Grid-Status (Netzeinspeisung/-entnahme)
- Batterie-SOC (State of Charge)
- Sonnenauf-/-untergang (über Python-Skript `sunrise.py`)
- MQTT-Wärmepumpendaten
- Manuelle Leistungsvorgaben (`soyopower.txt`)

### read-inverter
Inverter-Register-Reader - liest Modbus-Register vom Sofar-Inverter:
- CSV-basierte Register-Definitionen
- Modbus RTU über Serial Port
- Filterung nach kW/kWh/% oder alle Register
- CSV-Export der Daten

Siehe `read-inverter/README.md` für Details.

## Abhängigkeiten

### C-Build-Abhängigkeiten

**Debian/Ubuntu/Raspberry Pi OS:**
```bash
sudo apt-get update
sudo apt-get install -y build-essential libmosquitto-dev
```

**Andere Distributionen:**
- **Fedora/RHEL**: `sudo dnf install gcc make mosquitto-devel`
- **Arch**: `sudo pacman -S gcc make mosquitto`

### Python-Abhängigkeiten (für sunrise.py)

Das C-Programm ruft `sunrise.py` auf, um Sonnenauf- und -untergangszeiten zu berechnen:

```bash
# Python 3 und pip installieren (falls nicht vorhanden)
sudo apt-get install python3 python3-pip

# Python-Pakete installieren
pip3 install -r requirements.txt

# Oder einzeln:
pip3 install astral pytz
```

## Build

```bash
cd c-version

# Optional: Prüfe System-Requirements
./configure

# Kompilieren
make
```

Für Debug-Build:
```bash
make debug
```

## Installation

```bash
sudo make install
```

Dies installiert die Binärdatei nach `/usr/local/bin/soyo1min`.

## Konfiguration

Alle Konfigurationsparameter befinden sich in `config.h`:

- **SERIAL_PORT**: Serieller Port für Inverter-Kommunikation (Standard: `/dev/ttyUSB32`)
- **MQTT_BROKER**: MQTT-Broker-IP (Standard: `192.168.178.218`)
- **MQTT_TOPIC**: MQTT-Topic für Wärmepumpen-Leistung (Standard: `em0/54`)
- **BATTERY_CAPACITY_KWH**: Batteriekapazität in kWh (Standard: `30`)
- **NIGHT_STANDARD_POWER**: Grundlast während Nachtzeit in W (Standard: `390`)
- **BAT2_SOC_MIN**: Minimaler SOC für Entladeschutz (Standard: `9%`)

Nach Änderungen neu kompilieren:
```bash
make clean
make
```

## Verwendung

```bash
# Direkt starten
./soyo1min

# Mit systemd (empfohlen)
sudo systemctl start soyo1min
```

## Logging

Logs werden geschrieben nach: `/run/user/1000/soyo1min_c.log`

Log-Level:
- **DEBUG**: Detaillierte Informationen
- **INFO**: Normale Betriebsmeldungen
- **WARNING**: Warnungen (z.B. ungültige Daten)
- **ERROR**: Fehler

## Externe Abhängigkeiten

Das C-Programm ruft externe Skripte auf:
- **sunrise.py**: Muss im gleichen Verzeichnis liegen oder im PATH vorhanden sein

## Prioritäten-System

Das System arbeitet mit folgenden Prioritäten (von höchster bis niedrigster):

1. **Entladeschutz**: SOC < 9% → Leistung = 0W
2. **Bat2 Charging**: Batterie lädt (>2A) → Leistung = 0W
3. **Soyopower**: Manuelle Vorgabe aus `soyopower.txt`
4. **Feeding to Grid**: PV-Überschuss (Grid > 200W) → Leistung = 0W
5. **Buying from Grid**: Netzbezug (Grid < -100W) → Ausgleich durch Batterie
6. **Default**: Grundlast-Unterstützung

## Unterschiede zur Python-Version

- Logging erfolgt in separate Datei (`soyo1min_c.log`)
- Geringerer Speicher-Footprint
- Schnellere Ausführung
- Gleiche Funktionalität

## Fehlersuche

### Programm startet nicht
```bash
# Lock-File prüfen
cat /run/user/1000/soyo1min.lock

# Lock-File löschen (nur wenn sicher keine Instanz läuft)
rm /run/user/1000/soyo1min.lock
```

### Serielle Port-Fehler
```bash
# Berechtigungen prüfen
ls -l /dev/ttyUSB32

# User zur dialout-Gruppe hinzufügen
sudo usermod -a -G dialout $USER
```

### MQTT-Verbindungsprobleme
```bash
# MQTT-Broker testen
mosquitto_sub -h 192.168.178.218 -t em0/54 -v
```

## Systemd Service

Beispiel-Service-Datei (`/etc/systemd/system/soyo1min.service`):

```ini
[Unit]
Description=Soyo Battery Management (C version)
After=network.target

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/sofar
ExecStart=/usr/local/bin/soyo1min
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Aktivieren:
```bash
sudo systemctl daemon-reload
sudo systemctl enable soyo1min
sudo systemctl start soyo1min
```

## Lizenz

Wie Original-Python-Version.

## Version

**v1.9_FIXED_GRID_SIGN_DATETIME_C**
