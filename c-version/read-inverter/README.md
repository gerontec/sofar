# Read Inverter - C Version

C-Implementierung des Sofar-Inverter Register-Readers (read.py).

## Überblick

Dieses Programm liest Register von einem Sofar-Inverter über Modbus RTU aus:
- Liest Register-Definitionen aus CSV-Datei
- Verbindet sich über seriellen Port (Modbus RTU)
- Liest verschiedene Register-Bereiche mit Mask-Filterung
- Dekodiert Werte (U16, I16, U32, I32, U64, BCD16, ASCII)
- Speichert Ergebnisse in CSV-Dateien

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

Die wichtigsten Einstellungen sind bereits konfiguriert für Produktionsumgebung:

```c
#define SERIAL_PORT "/dev/ttyUSB32"                    // Serieller Port
#define BAUD_RATE 9600                                 // Baudrate
#define UNIT_ID 1                                      // Modbus Unit-ID
#define CSV_FILE "/home/pi/python/sofarregister.csv"  // Register-Definitionen (fest)
#define ALLREG 0                                       // 0=nur kW/kWh/%, 1=alle Register
```

**Standard-Konfiguration:**
- CSV-Datei ist fest auf `/home/pi/python/sofarregister.csv` eingestellt
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

# Direkt ausführen
./read_inverter

# Als root falls Serial-Port Berechtigung benötigt
sudo ./read_inverter
```

**Tipp:** Mit `--help` sehen Sie alle aktuell kompilierten Einstellungen, ohne in die Quelldateien schauen zu müssen.

## Output-Dateien

- `/tmp/raw.csv` - Rohe Registerdaten im CSV-Format
- Konsolenausgabe mit allen gelesenen Registern

## Register-Filter

Standardmäßig (`ALLREG=0`) werden nur Register mit folgenden Einheiten gelesen:
- `kW` (Kilowatt)
- `kWh` (Kilowattstunden)
- `%` (Prozent, für SOC)

Setzen Sie `ALLREG=1` in `read_config.h` um alle Register zu lesen.

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

### Error: Failed to create Modbus context
```bash
# libmodbus installieren
sudo apt-get install libmodbus-dev
```

### Error: Cannot open CSV file
```bash
# Pfad in read_config.h anpassen
# CSV-Datei muss existieren und lesbar sein
```

### No registers read
```bash
# ALLREG auf 1 setzen in read_config.h
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

❌ **Nicht implementiert:**
- Pivotierte CSV-Ausgabe (nur raw.csv)
- Pandas-Integration
- Logging in Datei (nur stdout/stderr)

## Installation (optional)

```bash
sudo make install
```

Installiert nach `/usr/local/bin/read_inverter`.

## Version

C-Portierung von read.py v1.0
