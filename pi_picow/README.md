# E-Box Battery Monitor für Raspberry Pi Pico W

Liest Batteriedaten von einer E-Box über RS232 (UART) und sendet den SOC (State of Charge) über MQTT an einen Broker.

## 🔧 Hardware-Anforderungen

- **Raspberry Pi Pico W** (mit WiFi)
- **RS232 zu TTL Konverter Modul** (z.B. MAX3232)
- **E-Box** mit serieller Schnittstelle
- **WiFi Netzwerk**
- **MQTT Broker** (z.B. Mosquitto auf 192.168.178.218)

## 📋 Verkabelung

### RS232 Modul an Pico W

| RS232 Modul | Pico W Pin | GPIO |
|-------------|------------|------|
| TX          | Pin 2      | GP1  |
| RX          | Pin 1      | GP0  |
| GND         | Pin 3, 8, 13, 18, 23, 28, 33, 38 | GND  |
| VCC (3.3V)  | Pin 36     | 3V3  |

### E-Box an RS232 Modul

Verbinden Sie die serielle Schnittstelle der E-Box mit dem RS232 Modul:
- E-Box TX → RS232 RX
- E-Box RX → RS232 TX
- E-Box GND → RS232 GND

## 🛠️ Software-Anforderungen

### Pico SDK Installation

```bash
# Debian/Ubuntu/Raspberry Pi OS
sudo apt update
sudo apt install -y cmake gcc-arm-none-eabi libnewlib-arm-none-eabi build-essential

# Pico SDK herunterladen
cd ~
git clone https://github.com/raspberrypi/pico-sdk.git
cd pico-sdk
git submodule update --init

# Umgebungsvariable setzen
export PICO_SDK_PATH=~/pico-sdk
echo 'export PICO_SDK_PATH=~/pico-sdk' >> ~/.bashrc
```

## 📝 Konfiguration

1. **WiFi und MQTT konfigurieren:**

Bearbeiten Sie `wifi_config.h` und tragen Sie Ihre Daten ein:

```c
#define WIFI_SSID "IhrWiFiName"
#define WIFI_PASSWORD "IhrWiFiPasswort"

#define MQTT_BROKER_IP "192.168.178.218"
#define MQTT_BROKER_PORT 1883
```

2. **UART Pins anpassen (optional):**

Falls Sie andere GPIO Pins verwenden möchten, ändern Sie in `wifi_config.h`:

```c
#define UART_TX_PIN 0    // GPIO für TX
#define UART_RX_PIN 1    // GPIO für RX
```

## 🏗️ Kompilieren

```bash
cd pi_picow

# Build-Verzeichnis erstellen
mkdir build
cd build

# CMake konfigurieren
cmake ..

# Kompilieren
make

# Ergebnis: build/ebox_pico.uf2
```

## 📥 Flashen auf Pico W

1. **BOOTSEL Modus aktivieren:**
   - Halten Sie die **BOOTSEL** Taste auf dem Pico W gedrückt
   - Verbinden Sie das USB-Kabel mit dem Computer
   - Lassen Sie BOOTSEL los
   - Der Pico W erscheint als USB-Laufwerk **RPI-RP2**

2. **Firmware kopieren:**
   ```bash
   cp build/ebox_pico.uf2 /media/$USER/RPI-RP2/
   ```

   Oder per Drag & Drop die Datei `ebox_pico.uf2` auf das RPI-RP2 Laufwerk ziehen.

3. **Der Pico W startet automatisch** nach dem Kopieren neu.

## 🖥️ Debugging / Serieller Monitor

Um die Debug-Ausgaben zu sehen, verbinden Sie sich mit dem USB Serial Port:

```bash
# Linux
sudo apt install minicom
minicom -D /dev/ttyACM0 -b 115200

# Oder mit screen
screen /dev/ttyACM0 115200

# macOS
screen /dev/cu.usbmodem* 115200

# Windows
# Verwenden Sie PuTTY oder einen anderen Serial Terminal
```

## 📡 MQTT Topics

Die Software publiziert auf folgende MQTT Topics:

| Topic | Beschreibung | Beispiel |
|-------|--------------|----------|
| `ebox/soc` | State of Charge in % | `85.5` |
| `ebox/voltage` | Spannung in mV | `52400.0` |
| `ebox/current` | Strom in mA | `1250.0` |
| `ebox/status` | Statusmeldungen | `connected` |

### MQTT testen

```bash
# Subscribe auf alle E-Box Topics
mosquitto_sub -h 192.168.178.218 -t "ebox/#" -v

# Oder nur SOC
mosquitto_sub -h 192.168.178.218 -t "ebox/soc" -v
```

## 🔄 Funktionsweise

1. **Beim Start:**
   - Initialisierung von UART (RS232)
   - Verbindung zum WiFi
   - Verbindung zum MQTT Broker

2. **Hauptschleife (alle 60 Sekunden):**
   - Sendet `bat` Kommando an E-Box via UART
   - Liest Antwort Zeile für Zeile
   - Parst Batteriedaten (Spannung, Strom, SOC)
   - Sendet Daten via MQTT

3. **LED Blink-Muster:**
   - **Schnell (250ms):** WiFi verbindet
   - **Langsam (1000ms):** Alles OK, verbunden
   - **Sehr schnell (100ms):** Fehler (MQTT nicht verbunden)

## 🐛 Troubleshooting

### Pico W verbindet sich nicht mit WiFi

- Überprüfen Sie SSID und Passwort in `wifi_config.h`
- Stellen Sie sicher, dass 2.4 GHz WiFi verfügbar ist (5 GHz wird nicht unterstützt)
- Prüfen Sie Debug-Ausgabe über USB Serial

### Keine Daten von E-Box

- Prüfen Sie die Verkabelung (RX/TX vertauscht?)
- Testen Sie die Baudrate (Standard: 115200)
- Prüfen Sie ob RS232 Modul mit Strom versorgt wird
- Sehen Sie sich UART Debug-Ausgaben an

### MQTT Verbindung fehlgeschlagen

- Prüfen Sie ob MQTT Broker erreichbar ist:
  ```bash
  ping 192.168.178.218
  ```
- Testen Sie MQTT Broker:
  ```bash
  mosquitto_pub -h 192.168.178.218 -t "test" -m "hello"
  ```
- Firewall-Einstellungen prüfen (Port 1883)

### Kompilierungsfehler

- Stellen Sie sicher, dass `PICO_SDK_PATH` gesetzt ist:
  ```bash
  echo $PICO_SDK_PATH
  ```
- Prüfen Sie ob alle Submodule geladen sind:
  ```bash
  cd $PICO_SDK_PATH
  git submodule update --init
  ```

## 📊 Leistungsdaten

- **Leseintervall:** 60 Sekunden (konfigurierbar)
- **Timeout E-Box:** 4 Sekunden
- **Retry bei Timeout:** 3 Versuche
- **Stromverbrauch:** ~50-100mA (WiFi aktiv)

## 🔐 Anpassungen

### Leseintervall ändern

In `ebox_pico.c` Zeile ändern:

```c
const uint32_t read_interval_ms = 60000;  // 60 Sekunden
```

### E-Box Kommando ändern

In `wifi_config.h`:

```c
#define EBOX_COMMAND "bat"  // oder "bat 1", "pwr", etc.
```

### Weitere MQTT Topics hinzufügen

Fügen Sie in `ebox_pico.c` in der Funktion `publish_mqtt()` hinzu:

```c
snprintf(payload, sizeof(payload), "%.1f", data->temperature);
mqtt_publish(mqtt_client, "ebox/temperature", payload, strlen(payload),
            0, 0, mqtt_pub_request_cb, NULL);
```

## 📄 Lizenz

Gleiche Lizenz wie das ursprüngliche ebox.c Projekt.

## ℹ️ Version

**v1.0.0-pico** - Raspberry Pi Pico W Port von ebox.c

---

## 📚 Weitere Ressourcen

- [Pico SDK Dokumentation](https://www.raspberrypi.com/documentation/microcontrollers/c_sdk.html)
- [Pico W Datasheet](https://datasheets.raspberrypi.com/picow/pico-w-datasheet.pdf)
- [MQTT.org](https://mqtt.org/)
- [Original ebox.c](../ebox.c)
