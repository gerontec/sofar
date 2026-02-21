# Pico W Inverter Management System

Komplettes Battery Management System für Raspberry Pi Pico W mit MQTT-basierter Kommunikation.

## 📦 Übersicht

Dieses Projekt enthält drei Programme für den Raspberry Pi Pico W:

1. **read_inverter_pico** - Liest Sofar Inverter via RS232/Modbus und publiziert Daten auf MQTT
2. **soyo1min_pico** - Empfängt MQTT-Daten und steuert Soyo Inverter via RS485
3. **fox2db_pico** - Power Management Controller mit Relay-Steuerung basierend auf MQTT-Daten

## 🎯 System-Architektur

```
┌──────────────┐      RS232      ┌──────────────────┐
│   Sofar      │──────Modbus─────▶│ read_inverter_   │
│   Inverter   │                  │ pico (Pico W #1) │
└──────────────┘                  └────────┬─────────┘
                                           │ WiFi
                                           ▼
                                    ┌──────────────┐
                                    │ MQTT Broker  │
                                    │ 192.168...   │
                                    └──────┬───────┘
                           ┌───────────────┴───────────────┐
                           │ WiFi                    WiFi  │
                           ▼                               ▼
              ┌─────────────────────┐        ┌──────────────────┐
              │ soyo1min_pico       │        │ fox2db_pico      │
              │ (Pico W #2)         │        │ (Pico W #3)      │
              └──────────┬──────────┘        └────────┬─────────┘
                    RS485 │                     GPIO   │
                          ▼                            ▼
                   ┌──────────────┐           ┌──────────────┐
                   │ Soyo Inverter│           │ 3x Relays    │
                   └──────────────┘           │ (State 0-7)  │
                                              └──────────────┘
```

## 🔧 Hardware-Anforderungen

### Gemeinsam für alle Programme
- **3x Raspberry Pi Pico W** (mit WiFi)
- **WiFi Netzwerk** (2.4 GHz)
- **MQTT Broker** (z.B. Mosquitto auf 192.168.178.218)

### Für read_inverter_pico
- **RS232 zu TTL Konverter** (z.B. MAX3232)
- **Sofar Inverter** mit Modbus RTU Schnittstelle

### Für soyo1min_pico
- **RS485 zu TTL Konverter** mit DE/RE Pins (z.B. MAX485)
- **Soyo Inverter** (empfängt nur, kein Feedback)

### Für fox2db_pico
- **3x Relais-Module** (oder 8 separate Relais)
- **Optionale Pegelwandler** bei 5V Relais

---

## 📋 Hardware-Verkabelung

### 1. read_inverter_pico (Sofar Reader)

#### RS232 Modul an Pico W

| RS232 Modul | Pico W Pin | GPIO | Beschreibung |
|-------------|------------|------|--------------|
| TX          | Pin 2      | GP1  | Receive Data |
| RX          | Pin 1      | GP0  | Transmit Data |
| GND         | Pin 3, 8, 13, etc. | GND  | Ground |
| VCC (3.3V)  | Pin 36     | 3V3  | Power (3.3V) |

#### Sofar Inverter an RS232 Modul

- Sofar TX → RS232 RX
- Sofar RX → RS232 TX
- Sofar GND → RS232 GND

**Modbus Konfiguration:**
- Baudrate: 9600 (Standard für Sofar)
- Data bits: 8
- Parity: None
- Stop bits: 1
- Unit ID: 1

---

### 2. soyo1min_pico (Soyo Controller)

#### RS485 Modul an Pico W

| RS485 Modul | Pico W Pin | GPIO | Beschreibung |
|-------------|------------|------|--------------|
| TX (DI)     | Pin 6      | GP4  | Transmit Data |
| RX (RO)     | Pin 7      | GP5  | Receive Data (nicht genutzt) |
| DE          | Pin 9      | GP6  | Driver Enable |
| RE          | Pin 9      | GP6  | Receiver Enable (mit DE verbunden) |
| GND         | Pin 3, 8, 13, etc. | GND  | Ground |
| VCC (3.3V)  | Pin 36     | 3V3  | Power (3.3V) |

**WICHTIG:** DE und RE Pins sollten zusammen verbunden sein und an GPIO 6!

#### Soyo Inverter an RS485 Modul

- RS485 A → Soyo A
- RS485 B → Soyo B
- RS485 GND → Soyo GND (optional, je nach Inverter)

**RS485 Konfiguration:**
- Baudrate: 4800 (Soyo Standard)
- Data bits: 8
- Parity: None
- Stop bits: 1
- **Hinweis:** Soyo Inverter sendet keine Rückmeldung!

---

### 3. fox2db_pico (Relay Controller)

#### Relais an Pico W (3-Bit Binary Control)

| Pico W Pin | GPIO | Funktion | Relay State |
|------------|------|----------|-------------|
| Pin 4      | GP2  | Bit 0 (LSB) | State 1, 3, 5, 7 |
| Pin 5      | GP3  | Bit 1       | State 2, 3, 6, 7 |
| Pin 6      | GP4  | Bit 2 (MSB) | State 4, 5, 6, 7 |
| Pin 3, 8   | GND  | Ground      | - |

**Binary State Table:**

| State | Power (W) | Bit 2 | Bit 1 | Bit 0 | Decimal |
|-------|-----------|-------|-------|-------|---------|
| 0     | 0         | 0     | 0     | 0     | 0 |
| 1     | 3000      | 0     | 0     | 1     | 1 |
| 2     | 3650      | 0     | 1     | 0     | 2 |
| 3     | 6650      | 0     | 1     | 1     | 3 |
| 4     | 3900      | 1     | 0     | 0     | 4 |
| 5     | 7100      | 1     | 0     | 1     | 5 |
| 6     | 7800      | 1     | 1     | 0     | 6 |
| 7     | 11400     | 1     | 1     | 1     | 7 |

**Alternative:** 8 separate Relais an GP2-GP9 für direkte State-Kontrolle.

---

## 🛠️ Software-Installation

### Pico SDK installieren

```bash
# Debian/Ubuntu/Raspberry Pi OS
sudo apt update
sudo apt install -y cmake gcc-arm-none-eabi libnewlib-arm-none-eabi \
                    build-essential libstdc++-arm-none-eabi-newlib git

# Pico SDK herunterladen
cd ~
git clone https://github.com/raspberrypi/pico-sdk.git --branch master
cd pico-sdk
git submodule update --init

# Umgebungsvariable setzen
export PICO_SDK_PATH=~/pico-sdk
echo 'export PICO_SDK_PATH=~/pico-sdk' >> ~/.bashrc
source ~/.bashrc
```

---

## 📝 Konfiguration

### 1. Sofar Reader (read_inverter_pico)

Bearbeiten Sie `sofar_config.h`:

```c
// WiFi
#define SOFAR_WIFI_SSID "IhrWiFiName"
#define SOFAR_WIFI_PASSWORD "IhrWiFiPasswort"

// MQTT Broker
#define SOFAR_MQTT_BROKER "192.168.178.218"
#define SOFAR_MQTT_PORT 1883

// UART Pins (optional anpassen)
#define UART_MODBUS_TX_PIN 0     // GPIO 0
#define UART_MODBUS_RX_PIN 1     // GPIO 1
#define UART_MODBUS_BAUD 9600
```

### 2. Soyo Controller (soyo1min_pico)

Bearbeiten Sie `soyo_config.h`:

```c
// WiFi
#define SOYO_WIFI_SSID "IhrWiFiName"
#define SOYO_WIFI_PASSWORD "IhrWiFiPasswort"

// MQTT Broker
#define SOYO_MQTT_BROKER "192.168.178.218"
#define SOYO_MQTT_PORT 1883

// RS485 UART Pins (optional anpassen)
#define UART_RS485_TX_PIN 4     // GPIO 4
#define UART_RS485_RX_PIN 5     // GPIO 5
#define UART_RS485_DE_PIN 6     // GPIO 6 (DE/RE)
#define UART_RS485_BAUD 4800

// Power Limits
#define MAX_POWER 999                 // Max power to Soyo
#define NIGHT_STANDARD_POWER 390      // Base load at night
#define BAT2_SOC_MIN 9                // Min SOC (%)
```

### 3. Fox2DB Controller (fox2db_pico)

Bearbeiten Sie direkt in `fox2db_pico.c`:

```c
#define WIFI_SSID "IhrWiFiName"
#define WIFI_PASSWORD "IhrWiFiPasswort"
#define MQTT_BROKER "192.168.178.218"
#define MQTT_PORT 1883

// Relay GPIO Pins
#define RELAY_PIN_0 2  // LSB
#define RELAY_PIN_1 3
#define RELAY_PIN_2 4  // MSB

// Configuration
#define MIN_EXCESS 1010         // Min excess power (W)
#define MAX_GRID_DRAW 1500      // Max allowed grid draw (W)
#define MAX_SOC 99              // Max SOC (%)
```

---

## 🏗️ Kompilieren

```bash
cd /home/user/sofar/pi_picow

# Build-Verzeichnis erstellen (einmalig)
mkdir -p build
cd build

# CMake konfigurieren
cmake ..

# Alle Programme kompilieren
make

# Oder einzelne Programme:
make read_inverter_pico
make soyo1min_pico
make fox2db_pico
```

**Ergebnisse:**
- `build/read_inverter_pico.uf2`
- `build/soyo1min_pico.uf2`
- `build/fox2db_pico.uf2`

---

## 📥 Flashen auf Pico W

Für jeden Pico W:

1. **BOOTSEL Modus aktivieren:**
   - Halte **BOOTSEL** Taste gedrückt
   - Verbinde USB-Kabel
   - Lasse BOOTSEL los
   - Pico erscheint als **RPI-RP2** Laufwerk

2. **Firmware kopieren:**
   ```bash
   cp build/read_inverter_pico.uf2 /media/$USER/RPI-RP2/
   # Oder die anderen .uf2 Dateien
   ```

3. **Pico startet automatisch neu**

---

## 📡 MQTT Topics

### read_inverter_pico publiziert:

| Topic | Beschreibung | Beispiel |
|-------|--------------|----------|
| `sofar/grid_w` | Grid power (W) | `250` |
| `sofar/bat_w` | Battery power (W) | `-150` |
| `sofar/soc_bat1` | Battery 1 SOC (%) | `85` |
| `sofar/soc_bat2` | Battery 2 SOC (%) | `82` |
| `sofar/bat2_current` | Battery 2 current (A) | `15.50` |
| `sofar/state` | Inverter state (0-7) | `0` |
| `sofar/csv` | Full CSV line | `250,-150,82,85,15.50,0` |
| `sofar/status` | Status messages | `Sofar reader online` |

### soyo1min_pico subscribed:

| Topic | Beschreibung |
|-------|--------------|
| `sofar/grid_w` | Grid power |
| `sofar/soc_bat2` | Battery SOC |
| `sofar/bat2_current` | Battery current |
| `sofar/bat_w` | Battery power |
| `sofar/state` | Inverter state |
| `em0/54` | Heat pump power |

### soyo1min_pico publiziert:

| Topic | Beschreibung |
|-------|--------------|
| `soyo/power` | Current power sent to Soyo |
| `soyo/status` | Status messages |

### fox2db_pico subscribed:

| Topic | Beschreibung |
|-------|--------------|
| `inverter/pcc` | Grid exchange power |
| `inverter/bat1` | Battery power |
| `inverter/soc` | State of charge |
| `inverter/bat_cur` | Battery current |

### fox2db_pico publiziert:

| Topic | Beschreibung |
|-------|--------------|
| `fox2db/state` | Current relay state (0-7) |
| `fox2db/status` | Status messages |

---

## 🔍 Testen

### MQTT Broker testen

```bash
# Subscribe auf alle Topics
mosquitto_sub -h 192.168.178.218 -t "#" -v

# Nur Sofar Topics
mosquitto_sub -h 192.168.178.218 -t "sofar/#" -v

# Nur Soyo Topics
mosquitto_sub -h 192.168.178.218 -t "soyo/#" -v
```

### Daten manuell senden (für Tests)

```bash
# Simuliere Sofar Daten für Soyo Controller
mosquitto_pub -h 192.168.178.218 -t "sofar/grid_w" -m "250"
mosquitto_pub -h 192.168.178.218 -t "sofar/soc_bat2" -m "85"
mosquitto_pub -h 192.168.178.218 -t "sofar/bat2_current" -m "15.5"

# Simuliere Inverter Daten für Fox2DB
mosquitto_pub -h 192.168.178.218 -t "inverter/pcc" -m "1200"
mosquitto_pub -h 192.168.178.218 -t "inverter/soc" -m "80"
```

---

## 🖥️ Debugging

### USB Serial Monitor

```bash
# Linux
minicom -D /dev/ttyACM0 -b 115200
# Oder
screen /dev/ttyACM0 115200

# macOS
screen /dev/cu.usbmodem* 115200
```

### LED Status

**Alle Programme verwenden das gleiche LED Muster:**
- **Schnell blinken (250ms):** WiFi verbindet
- **Mittelschnell (100ms):** WiFi OK, MQTT verbindet
- **Langsam (1000ms):** Alles OK, voll funktionsfähig

---

## 🐛 Troubleshooting

### WiFi Verbindung fehlgeschlagen
- SSID/Passwort in Config-Dateien prüfen
- Nur 2.4 GHz wird unterstützt (5 GHz nicht)
- Router-Einstellungen prüfen (DHCP aktiv?)

### MQTT Verbindung fehlgeschlagen
```bash
# Broker erreichbar?
ping 192.168.178.218

# Broker läuft?
sudo systemctl status mosquitto

# Firewall?
sudo ufw allow 1883/tcp
```

### Keine Modbus-Daten vom Sofar
- RX/TX vertauscht? (häufigster Fehler!)
- Baudrate korrekt? (Standard: 9600)
- Modbus Unit ID korrekt? (Standard: 1)
- RS232 Modul mit Strom versorgt?

### RS485 sendet nicht zum Soyo
- A/B Leitungen vertauscht?
- DE/RE Pin korrekt verkabelt?
- Baudrate 4800 eingestellt?
- Soyo Inverter eingeschaltet?

### Relais schalten nicht
- GPIO Pins korrekt?
- Relais-Modul mit Strom versorgt?
- Pegelwandler nötig? (3.3V → 5V)
- LED auf Relais-Modul blinkt?

---

## 📊 Systemparameter

### read_inverter_pico
- Update-Intervall: 2 Sekunden
- Modbus Timeout: 1 Sekunde
- Register: 6 (Grid, Bat, SOC1, SOC2, Current, State)

### soyo1min_pico
- Update-Intervall: 2 Sekunden
- MQTT Timeout: 60 Sekunden
- Max Power: 999 W
- Night Power: 390 W

### fox2db_pico
- Update-Intervall: 5 Sekunden
- Stabilization Cycles: 2
- Min Excess: 1010 W
- Max Grid Draw: 1500 W

---

## 📚 Weitere Informationen

- [Pico W Dokumentation](https://www.raspberrypi.com/documentation/microcontrollers/raspberry-pi-pico.html)
- [MQTT Dokumentation](https://mqtt.org/)
- [Modbus RTU Specification](https://modbus.org/specs.php)

---

## 📄 Lizenz

Gleiche Lizenz wie die Original-Projekte (soyo1min.c, read_inverter.c, fox2db.c)

## ℹ️ Version

**v1.0.0-pico** - Raspberry Pi Pico W Ports

---

**Erstellt:** 2026-02-16
**Für:** Raspberry Pi Pico W mit Pico SDK
