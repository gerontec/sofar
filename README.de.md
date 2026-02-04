# Sofar Inverter Management System

[🇬🇧 English Version](README.md)

Batterie-Management und Überwachungssystem für Sofar Wechselrichter mit umfassender Modbus RTU Integration.

## Überblick

Dieses Projekt bietet Tools für die Verwaltung und Überwachung von Sofar Solar-Wechselrichtern:

- **Batterie-Management** (`soyo1min`) - Intelligente Batterieentladungssteuerung basierend auf Netzstatus, SOC, Tageszeit und Wärmepumpen-Integration
- **Register-Reader** (`read_inverter`) - Umfassender Modbus RTU Register-Reader mit CSV-Export

## Features

### Batterie-Management-System (soyo1min)
- Echtzeit-Batterieentladungssteuerung
- Netzstatus-Überwachung (Einspeisung/Bezug)
- SOC-basierter Schutz (verhindert Tiefentladung)
- Sonnenauf-/-untergangs-Integration
- MQTT-Wärmepumpendaten-Integration
- Manuelle Leistungssteuerung via Datei
- Prioritäts-basiertes Entscheidungssystem

### Register-Reader (read_inverter)
- Liest Modbus RTU Register vom Sofar-Wechselrichter
- CSV-basierte Registerdefinitionen (zur Build-Zeit eingebettet)
- Mehrere Datentypen: U16, I16, U32, I32, U64, BCD16, ASCII
- Filterung nach Einheit (kW/kWh/%) oder alle Register
- **Vollständige Kommandozeilen-Konfiguration** (NEU!)
- CSV-Export der Messwerte
- Produktionsreif mit eingebetteten Registerdefinitionen

## Schnellstart

```bash
# Abhängigkeiten installieren
sudo apt-get update
sudo apt-get install -y build-essential libmosquitto-dev libmodbus-dev python3 python3-pip

# Python-Abhängigkeiten installieren
pip3 install astral pytz

# Build
cd c-version
make

# Batterie-Management starten
./soyo1min

# Wechselrichter-Register mit eigenen Optionen auslesen
cd read-inverter
./read_inverter --help
./read_inverter --port /dev/ttyUSB0 --all-registers
```

## Dokumentation

- [C Version README](c-version/README.md) - Details zum Batterie-Management-System
- [Installationsanleitung](c-version/INSTALL.md) - Detaillierte Installationsanweisungen
- [Register-Reader](c-version/read-inverter/README.md) - Wechselrichter-Register-Reader mit CLI-Optionen

## Voraussetzungen

### Hardware
- Serieller Port (z.B. `/dev/ttyUSB32` oder `/dev/ttyUSB0`)
- MQTT-Broker (für Wärmepumpen-Integration)
- Sofar-Wechselrichter mit Modbus RTU

### Software
- Linux (Raspberry Pi OS, Debian, Ubuntu, etc.)
- GCC-Compiler
- libmosquitto-dev (für MQTT)
- libmodbus-dev (für Modbus RTU)
- Python 3 mit astral und pytz (für Sonnenauf-/-untergangsberechnung)

### Berechtigungen
Benutzer muss in der `dialout`-Gruppe sein:
```bash
sudo usermod -a -G dialout $USER
# Aus- und wieder einloggen
```

## Architektur

Das System besteht aus zwei C-Hauptprogrammen:

### 1. soyo1min - Batterie-Management
Befindet sich in `c-version/`

**Entscheidungspriorität (von höchster bis niedrigster):**
1. Entladeschutz: SOC < 9% → Leistung = 0W
2. Batterie lädt: Batterie lädt (>2A) → Leistung = 0W
3. Manuelle Vorgabe: Leistung aus `soyopower.txt`
4. Netzeinspeisung: PV-Überschuss (Grid > 200W) → Leistung = 0W
5. Netzbezug: Bezug aus Netz (Grid < -100W) → Ausgleich durch Batterie
6. Standard: Grundlast-Unterstützung

**Wichtige Dateien:**
- `config.h` - Alle Konfigurationsparameter
- `soyo1min.c` - Hauptprogrammlogik
- `sunrise.py` - Python-Skript für Sonnenauf-/-untergangsberechnung

### 2. read_inverter - Register-Reader
Befindet sich in `c-version/read-inverter/`

**Features:**
- Eingebettete Registerdefinitionen (keine externe CSV zur Laufzeit benötigt)
- Vollständige Kommandozeilen-Konfiguration
- Masken-basierte Registerfilterung für effizientes Auslesen
- Mehrere Ausgabeformate

**Wichtige Dateien:**
- `read_config.h` - Standard-Konfigurationswerte
- `read_inverter.c` - Hauptprogramm mit CLI-Parsing
- `sofarregister.csv` - Registerdefinitionen (zur Build-Zeit eingebettet)

## Konfiguration

### Batterie-Management (soyo1min)
Editieren Sie `c-version/config.h`:
```c
#define SERIAL_PORT "/dev/ttyUSB32"      // Serieller Port
#define MQTT_BROKER "192.168.178.218"    // MQTT-Broker-IP
#define MQTT_TOPIC "em0/54"              // Wärmepumpen-Leistungs-Topic
#define BATTERY_CAPACITY_KWH 30          // Batteriekapazität
#define BAT2_SOC_MIN 9                   // Minimaler SOC (%)
```

### Register-Reader (read_inverter)
**NEU: Kommandozeilen-Konfiguration!** Keine Neukompilierung nötig:

```bash
# Serielle Kommunikation
./read_inverter --port /dev/ttyUSB0 --baud 9600 --unit-id 1

# Dateipfade
./read_inverter --csv custom_registers.csv --output /tmp/data.csv

# Register-Optionen
./read_inverter --all-registers  # Alle Register lesen
./read_inverter --filter         # Nur kW/kWh/% (Standard)

# Erweiterte Optionen
./read_inverter --max-register 0x3000 --block-size 64
```

Oder editieren Sie `c-version/read-inverter/read_config.h` für Standardwerte.

## Build

```bash
cd c-version

# System-Voraussetzungen prüfen (optional)
./configure

# Alle Programme bauen
make

# Mit Debug-Symbolen bauen
make debug

# Nach /usr/local/bin installieren
sudo make install
```

## Logging

- **soyo1min**: `/run/user/1000/soyo1min_c.log`
- **read_inverter**: stdout/stderr

Log-Level: DEBUG, INFO, WARNING, ERROR

## Systemd-Service

Beispiel-Service für Batterie-Management:

```bash
# Binary installieren
sudo make install

# Service-Datei erstellen
sudo nano /etc/systemd/system/soyo1min.service
```

```ini
[Unit]
Description=Sofar Batterie-Management
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

```bash
# Aktivieren und starten
sudo systemctl daemon-reload
sudo systemctl enable soyo1min
sudo systemctl start soyo1min

# Status prüfen
sudo systemctl status soyo1min
```

## Fehlersuche

### Serieller Port-Zugriff
```bash
# Berechtigungen prüfen
ls -l /dev/ttyUSB32

# User zur dialout-Gruppe hinzufügen
sudo usermod -a -G dialout $USER
# Aus- und wieder einloggen erforderlich!
```

### MQTT-Verbindungsprobleme
```bash
# MQTT-Broker testen
mosquitto_sub -h 192.168.178.218 -t em0/54 -v
```

### Lock-Datei-Probleme
```bash
# Prüfen ob eine andere Instanz läuft
ps aux | grep soyo1min

# Lock-Datei entfernen (nur wenn keine Instanz läuft)
rm /run/user/1000/soyo1min.lock
```

### Build-Fehler
```bash
# Fehlende Abhängigkeiten installieren
sudo apt-get install -y build-essential libmosquitto-dev libmodbus-dev

# Sauber neu bauen
make clean
make
```

## Performance

Vorteile der C-Implementierung:
- 10-20x schnellerer Start
- 50% weniger Speicherverbrauch (2-3 MB vs 15-20 MB Python)
- Gleiche Funktionalität wie Python-Version
- Produktionsreif

## Projektstruktur

```
sofar/
├── README.md                    # Diese Datei (Englisch)
├── README.de.md                 # Deutsche Version
├── c-version/
│   ├── README.md                # Batterie-Management (Englisch)
│   ├── README.de.md             # Batterie-Management (Deutsch)
│   ├── INSTALL.md               # Installationsanleitung (Englisch)
│   ├── INSTALL.de.md            # Installationsanleitung (Deutsch)
│   ├── config.h                 # Konfiguration
│   ├── soyo1min.c               # Haupt-Batterie-Management
│   ├── Makefile                 # Build-System
│   └── read-inverter/
│       ├── README.md            # Register-Reader (Englisch)
│       ├── README.de.md         # Register-Reader (Deutsch)
│       ├── read_config.h        # Standard-Konfiguration
│       ├── read_inverter.c      # Hauptprogramm mit CLI
│       ├── csv_to_c.py          # Bettet CSV zur Build-Zeit ein
│       └── Makefile             # Build-System
└── python/                      # Original Python-Version
```

## Mitwirken

Dieses Projekt wurde für den Produktionseinsatz auf Raspberry Pi mit Sofar-Wechselrichtern entwickelt.

Bei Änderungen:
1. Gründlich auf Ziel-Hardware testen
2. Sowohl englische als auch deutsche Dokumentation aktualisieren
3. Rückwärtskompatibilität mit bestehenden Config-Dateien bewahren
4. Bestehenden Code-Stil befolgen

## Version

**Aktuell: v1.9_FIXED_GRID_SIGN_DATETIME_C**

Neueste Updates:
- Umfassende Kommandozeilen-Argument-Unterstützung für read_inverter hinzugefügt
- Alle Konfigurationsparameter jetzt via CLI verfügbar
- Rückwärtskompatibilität mit Header-Datei-Defaults beibehalten
- Detaillierte Hilfe mit Beispielen hinzugefügt

## Lizenz

Wie ursprüngliche Python-Version.

## Support

Bei Problemen bitte prüfen:
1. Log-Dateien in `/run/user/1000/`
2. Serieller Port-Berechtigungen
3. MQTT-Broker-Konnektivität
4. Konfigurationswerte

Stellen Sie sicher, dass alle Abhängigkeiten installiert sind und der Benutzer die richtigen Berechtigungen hat.
