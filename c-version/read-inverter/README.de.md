# Read Inverter - C Version

[🇬🇧 English Version](README.md)

C-Implementierung des Sofar-Inverter Register-Readers (read.py).

## Überblick

Dieses Programm liest Register von einem Sofar-Inverter über Modbus RTU aus:
- Liest Register-Definitionen aus CSV-Datei (zur Build-Zeit eingebettet)
- Verbindet sich über seriellen Port (Modbus RTU)
- Liest verschiedene Register-Bereiche mit Mask-Filterung
- Dekodiert Werte (U16, I16, U32, I32, U64, BCD16, ASCII)
- Speichert Ergebnisse in CSV-Dateien
- **Vollständige Kommandozeilen-Konfiguration** (NEU!)

## Abhängigkeiten

### Debian/Ubuntu
```bash
sudo apt-get update
sudo apt-get install -y build-essential libmodbus-dev
```

### Andere Distributionen
- **Fedora/RHEL**: `sudo dnf install gcc make libmodbus-devel`
- **Arch**: `sudo pacman -S gcc make libmodbus`

## Konfiguration

### NEU: Kommandozeilen-Argumente

Die gesamte Konfiguration kann jetzt über die Kommandozeile erfolgen, ohne Neukompilierung:

```bash
# Alle Optionen und aktuelle Standardwerte anzeigen
./read_inverter --help

# Serielle Kommunikation
./read_inverter --port /dev/ttyUSB0 --baud 9600 --unit-id 1
./read_inverter -p /dev/ttyUSB0 -b 9600 -u 1      # Kurzform

# Timeout-Einstellungen
./read_inverter --timeout 2                        # Sekunden
./read_inverter --timeout-usec 500000              # Mikrosekunden

# Dateipfade
./read_inverter --csv /pfad/zu/custom_registers.csv
./read_inverter --output /tmp/meine_ausgabe.csv
./read_inverter --pivoted-output /tmp/pivoted.csv

# Register-Optionen
./read_inverter --all-registers                    # Alle Register lesen
./read_inverter -a                                 # Kurzform
./read_inverter --filter                           # Nur kW/kWh/% (Standard)
./read_inverter -f                                 # Kurzform

# Erweiterte Optionen
./read_inverter --max-register 0x3000              # Maximale Registeradresse (hex)
./read_inverter --block-size 64                    # Modbus-Blockgröße (1-125)

# Optionen kombinieren
./read_inverter -p /dev/ttyUSB0 -b 9600 -a -o /tmp/vollständiger_scan.csv
```

### Standard-Konfiguration

Die wichtigsten Einstellungen sind bereits konfiguriert für Produktionsumgebung in `read_config.h`:

```c
#define SERIAL_PORT "/dev/ttyUSB32"                    // Serieller Port
#define BAUD_RATE 9600                                 // Baudrate
#define UNIT_ID 1                                      // Modbus Unit-ID
#define CSV_FILE "/home/pi/python/sofarregister.csv"  // Register-Definitionen
#define ALLREG 0                                       // 0=nur kW/kWh/%, 1=alle Register
```

**Embedded Register-Definitionen:**
- Beim Build werden alle Register-Definitionen aus `/home/pi/python/sofarregister.csv` direkt ins Binary eingebettet
- Das kompilierte Programm benötigt **keine externe CSV-Datei** mehr zur Laufzeit
- Die Binary ist standalone und vollständig portabel
- Falls die CSV zur Build-Zeit nicht verfügbar ist, fällt das Programm auf Runtime-CSV-Loading zurück

**Standard-Konfiguration:**
- Filter-Modus: nur kW/kWh/% Register (schneller, weniger Daten)
- Diese Werte sind produktionsreif und müssen normalerweise nicht geändert werden

**Optional:** Nur falls Sie andere Werte benötigen, editieren Sie `read_config.h` und kompilieren neu.

## Build

```bash
cd read-inverter

# Optional: System-Requirements prüfen
./configure

# Kompilieren
make
```

Für Debug-Build:
```bash
make debug
```

**Hinweis:** Falls `configure` nicht existiert, holen Sie die neueste Version:
```bash
git pull origin claude/python-to-c-conversion-fDGGt
```

## Verwendung

```bash
# Hilfe anzeigen (zeigt alle Konfigurationswerte)
./read_inverter --help

# Version anzeigen
./read_inverter --version

# Direkt mit Standardwerten ausführen
./read_inverter

# Mit eigenen Einstellungen ausführen
./read_inverter --port /dev/ttyUSB0 --all-registers --output /tmp/daten.csv

# Als root falls Serial-Port Berechtigung benötigt
sudo ./read_inverter
```

**Tipp:** Mit `--help` sehen Sie alle aktuell kompilierten Einstellungen, ohne in die Quelldateien schauen zu müssen.

## Kommandozeilen-Optionen

### Allgemeine Optionen
- `-h, --help` - Hilfenachricht anzeigen und beenden
- `-v, --version` - Versionsinformationen anzeigen und beenden

### Serielle Kommunikation
- `-p, --port <gerät>` - Serieller Port (Standard: /dev/ttyUSB32)
- `-b, --baud <rate>` - Baudrate (Standard: 9600)
- `-u, --unit-id <id>` - Modbus Unit-ID 0-247 (Standard: 1)
- `-t, --timeout <sek>` - Antwort-Timeout in Sekunden (Standard: 1)
- `--timeout-usec <usec>` - Antwort-Timeout Mikrosekunden (Standard: 0)

### Dateipfade
- `-c, --csv <datei>` - Register-Definitions-CSV (Standard: /home/pi/python/sofarregister.csv)
- `-o, --output <datei>` - Rohdaten-Ausgabe-CSV (Standard: /tmp/raw.csv)
- `--pivoted-output <datei>` - Pivotierte Ausgabe-CSV (Standard: /tmp/pivoted_registers.csv)

### Register-Optionen
- `-a, --all-registers` - Alle Register lesen (Standard: nein)
- `-f, --filter` - Nur kW/kWh/% filtern (Gegenteil von --all-registers)
- `--max-register <addr>` - Maximale Registeradresse in Hex (Standard: 0x203F)
- `--block-size <größe>` - Modbus-Lese-Blockgröße 1-125 (Standard: 32)

## Beispiele

```bash
# Anderen seriellen Port und Unit-ID verwenden
./read_inverter -p /dev/ttyUSB0 -u 2

# Alle Register lesen und an eigenem Ort speichern
./read_inverter --all-registers --output /home/user/wechselrichter_daten.csv

# Eigene Register-Definitionen verwenden
./read_inverter -c /pfad/zu/meinen_registern.csv -o /tmp/ausgabe.csv

# Timeout für langsame Verbindungen anpassen
./read_inverter --timeout 3 --timeout-usec 0

# Vollständig eigene Konfiguration
./read_inverter \
  --port /dev/ttyUSB0 \
  --baud 9600 \
  --unit-id 1 \
  --all-registers \
  --output /tmp/vollständiger_scan.csv \
  --max-register 0x3000 \
  --block-size 64
```

## Output-Dateien

- `/tmp/raw.csv` - Rohe Registerdaten im CSV-Format
- Konsolenausgabe mit allen gelesenen Registern

## Register-Filter

Standardmäßig (`ALLREG=0` oder `--filter`) werden nur Register mit folgenden Einheiten gelesen:
- `kW` (Kilowatt)
- `kWh` (Kilowattstunden)
- `%` (Prozent, für SOC)

Verwenden Sie `--all-registers` oder `-a` um alle Register zu lesen.

## CSV-Format

Die Register-Definitions-CSV (`sofarregister.csv`) muss folgendes Format haben:
```
Sektion;Adresse;Name;Typ;Genauigkeit;Einheit
I General;0x0040-0x007F;Register Name;U16;1;kW
```

Spalten (durch `;` getrennt):
1. Sektion (leer für Register-Zeilen)
2. Adresse (Hex, kann Range sein: `0x0040-0x007F` oder `0x0040____0x007F`)
3. Name
4. Typ (U16, I16, U32, I32, U64, BCD16, ASCII)
5. Genauigkeit (Multiplikator, z.B. 0.1)
6. Einheit (z.B. kW, kWh, %, V, A)

## Berechtigungen

User muss Zugriff auf den seriellen Port haben:
```bash
sudo usermod -a -G dialout $USER
# Dann neu anmelden
```

## Troubleshooting

### Error: Cannot open serial port
```bash
# Prüfen ob Port existiert
ls -l /dev/ttyUSB32

# Berechtigung prüfen
groups  # sollte "dialout" enthalten
```

### Error: Invalid option
```bash
# Sicherstellen dass neueste Version verwendet wird
git pull

# Verfügbare Optionen prüfen
./read_inverter --help
```

### Error: Failed to create Modbus context
```bash
# libmodbus installieren
sudo apt-get install libmodbus-dev
```

### Error: Cannot open CSV file
```bash
# CSV-Datei-Speicherort angeben
./read_inverter --csv /pfad/zu/sofarregister.csv

# Oder Pfad in read_config.h anpassen und neu kompilieren
```

### No registers read
```bash
# --all-registers Flag verwenden
./read_inverter --all-registers

# Oder sicherstellen dass CSV Register mit kW/kWh/% enthält
```

## Performance

- Extrem schnell (C-native)
- Geringer Speicherverbrauch (~2-5 MB)
- Keine Python-Abhängigkeiten

## Unterschiede zur Python-Version

✅ **Gleiche Funktionalität:**
- CSV-Parsing
- Modbus RTU Kommunikation
- Register-Dekodierung (alle Typen)
- Mask-basierte Register-Filterung
- CSV-Output

➕ **Neue Features:**
- Vollständige Kommandozeilen-Konfiguration
- Keine Neukompilierung für Parameter-Änderungen nötig
- Eingebettete Register-Definitionen (optional)

❌ **Nicht implementiert:**
- Pivotierte CSV-Ausgabe (nur raw.csv)
- Pandas-Integration
- Datei-Logging (nur stdout/stderr)

## Installation (optional)

```bash
sudo make install
```

Installiert nach `/usr/local/bin/read_inverter`.

## Version

C-Portierung von read.py v1.0 mit Kommandozeilen-Konfigurations-Unterstützung.

## Changelog

### v1.1 (Aktuell)
- Umfassende Kommandozeilen-Argument-Unterstützung hinzugefügt
- Alle Konfigurationsparameter jetzt via CLI verfügbar
- Rückwärtskompatibilität mit Header-Defaults beibehalten
- Detaillierte Hilfe mit Beispielen hinzugefügt
- Verbesserte Fehlermeldungen für ungültige Parameter

### v1.0
- Initiale C-Portierung von Python
- Eingebettete Registerdefinitionen
- Produktionsreife Konfiguration
