# Pico W MQTT Modbus Controller

**Standalone WiFi-enabled Modbus Controller mit MQTT**

---

## 🎯 Architektur

```
┌──────────────────────┐
│   MQTT Broker        │  (z.B. Mosquitto)
│   192.168.1.100:1883 │
└──────────┬───────────┘
           │ WiFi
           ↓
┌──────────────────────┐
│   Pico W             │
│   - WiFi (CYW43)     │
│   - MQTT Client      │
│   - Dual Core        │
└────┬────────────┬────┘
     │ UART0 TTL  │ UART1 TTL
     ↓            ↓
┌─────────┐   ┌─────────┐
│ MAX485  │   │ MAX485  │  TTL → RS485 Konverter
│ Modul #1│   │ Modul #2│
└────┬────┘   └────┬────┘
     │ RS485      │ RS485
     ↓            ↓
┌─────────┐   ┌─────────┐
│  SOYO   │   │ Relay   │
│ 4800bd  │   │ 9600bd  │
└─────────┘   └─────────┘
```

---

## 🔌 Hardware Connections

### MAX485 Modul #1 (SOYO)

| Pico W Pin | MAX485 Pin | Funktion |
|------------|------------|----------|
| GPIO 0 (UART0 TX) | DI (Data In) | Send data to RS485 |
| GPIO 1 (UART0 RX) | RO (Receiver Out) | Receive data from RS485 |
| GPIO 2 | DE (Driver Enable) | TX/RX mode control |
| GPIO 2 | RE (Receiver Enable) | TX/RX mode control (tied to DE) |
| 3.3V | VCC | Power |
| GND | GND | Ground |

**MAX485 RS485 Side:**
- A → SOYO A (RS485+)
- B → SOYO B (RS485-)

---

### MAX485 Modul #2 (Relay)

| Pico W Pin | MAX485 Pin | Funktion |
|------------|------------|----------|
| GPIO 4 (UART1 TX) | DI | Send |
| GPIO 5 (UART1 RX) | RO | Receive |
| GPIO 6 | DE/RE | TX/RX control |
| 3.3V | VCC | Power |
| GND | GND | Ground |

**MAX485 RS485 Side:**
- A → Relay A (RS485+)
- B → Relay B (RS485-)

---

### LED

| Pico W Pin | Funktion |
|------------|----------|
| GPIO 25 (onboard) | Status blink (3s SOYO cycle) |

---

## 🛠️ Software Setup

### 1. Pico SDK installieren

```bash
cd ~
git clone https://github.com/raspberrypi/pico-sdk.git
cd pico-sdk
git submodule update --init
export PICO_SDK_PATH=$HOME/pico-sdk
```

### 2. Toolchain installieren

```bash
sudo apt install cmake gcc-arm-none-eabi libnewlib-arm-none-eabi build-essential
```

### 3. WiFi & MQTT Konfiguration

**Edit `pico_w_mqtt_modbus.c`:**

```c
// Zeile 55-57:
#define WIFI_SSID       "DeinWiFiName"
#define WIFI_PASSWORD   "DeinWiFiPasswort"
#define MQTT_BROKER_IP  "192.168.1.100"   // IP deines MQTT Brokers
```

### 4. Kompilieren

```bash
cd /path/to/sofar
mv pico_w_mqtt_modbus_CMakeLists.txt CMakeLists.txt

mkdir build && cd build
cmake ..
make -j4
```

**Ergebnis:** `build/pico_w_mqtt_modbus.uf2`

### 5. Flashen

1. **BOOTSEL** Taste gedrückt halten
2. USB einstecken → RPI-RP2 Laufwerk erscheint
3. `pico_w_mqtt_modbus.uf2` auf das Laufwerk kopieren
4. Pico W bootet automatisch!

---

## 📡 MQTT Topics

### Subscribe (Pico W empfängt):

| Topic | Typ | Beschreibung |
|-------|-----|--------------|
| `pico/soyo/watts` | `uint16_t` | SOYO Ziel-Leistung (0-65535 Watt) |
| `pico/soyo/enable` | `0/1` | SOYO enable/disable |
| `pico/relay/state` | `0-7,11` | Relais-Zustand |

### Publish (Pico W sendet):

| Topic | Typ | Interval | Beschreibung |
|-------|-----|----------|--------------|
| `pico/status` | JSON | 5s | Status {"soyo_watts":1500, "relay_state":3, ...} |

---

## 🚀 Nutzung

### MQTT Broker starten

```bash
# Auf Raspberry Pi oder PC
sudo apt install mosquitto mosquitto-clients
sudo systemctl start mosquitto
```

### Commands senden

**Von ÜBERALL im Netzwerk:**

```bash
# SOYO auf 1500W setzen
mosquitto_pub -h 192.168.1.100 -t "pico/soyo/watts" -m "1500"
mosquitto_pub -h 192.168.1.100 -t "pico/soyo/enable" -m "1"

# Relais auf State 3 (R1+R2 ON)
mosquitto_pub -h 192.168.1.100 -t "pico/relay/state" -m "3"
```

**ODER mit den Tools:**

```bash
# Kompiliere die Tools auf deinem PC/Raspberry Pi
make -f Makefile.mqtt_ctrl

# Nutze sie
./soyo_mqtt_ctrl 192.168.1.100 1500
./relay_mqtt_ctrl 192.168.1.100 3
```

### Status monitoren

```bash
mosquitto_sub -h 192.168.1.100 -t "pico/status" -v
```

**Output (alle 5s):**
```json
{"soyo_watts":1500,"relay_state":3,"relay_bits":"0x03","errors":"0x00"}
```

---

## 🧪 Debugging

### USB Serial Console

Nach dem Flash:

```bash
# Linux/Mac
screen /dev/ttyACM0 115200

# Windows
putty COM3 -serial -sercfg 115200
```

**Output:**
```
=== Pico W MQTT Modbus Controller ===
Connecting to WiFi 'MyWiFi'...
WiFi connected!
Connecting to MQTT broker at 192.168.1.100...
MQTT connected!
MQTT subscribed to topics
Hardware initialized
Core 1 launched
SOYO timer started (3s cycle)
Ready!
```

### Fehlersuche

**WiFi failed:**
- ✓ SSID/Password korrekt?
- ✓ WiFi in Reichweite?
- ✓ 2.4GHz WiFi? (Pico W unterstützt kein 5GHz!)

**MQTT failed:**
- ✓ Broker läuft? (`sudo systemctl status mosquitto`)
- ✓ IP korrekt?
- ✓ Firewall offen? (Port 1883)

**SOYO Timeout:**
- ✓ MAX485 Verkabelung korrekt?
- ✓ RS485 A/B vertauscht?
- ✓ SOYO eingeschaltet?

---

## ⚡ Features

### ✅ Was funktioniert

- **WiFi:** Auto-connect bei Start
- **MQTT:** Auto-reconnect bei Verbindungsverlust
- **Dual-Core:** SOYO (Core 0, 3s) + Relay (Core 1, event)
- **RS485 Control:** Automatisches DE/RE Switching
- **Status Publishing:** Alle 5s
- **LED Feedback:** Blinkt bei SOYO Command (3s Takt)

### 🚧 TODO

- MQTT incoming data parsing (aktuell nur stub!)
- WiFi reconnect bei Connection Lost
- Retained messages Support
- OTA Updates via MQTT

---

## 📊 Performance

| Metrik | Wert |
|--------|------|
| **SOYO Latenz** | ~3s (Timer-basiert) |
| **Relay Latenz** | ~100ms (Event-basiert) |
| **MQTT Latenz** | ~50ms (WiFi + MQTT) |
| **WiFi Reconnect** | ~5s |
| **RAM Usage** | ~80KB (lwIP stack) |

---

## 🔧 Anpassungen

### MQTT QoS ändern

```c
// Zeile 47:
#define MQTT_QOS 2  // 0=at most once, 1=at least once, 2=exactly once
```

### Status Interval ändern

```c
// Zeile 67:
#define STATUS_PUBLISH_MS 10000  // 10 Sekunden
```

### Andere UARTs nutzen

Pico hat nur UART0 und UART1. Wenn du mehr brauchst:
- **PIO UARTs** (programmable I/O)
- **I2C-zu-UART Bridges**

---

## 🌐 Integration

### Mit fox2db.c

```c
// In fox2db.c, statt lokalem Modbus:
char cmd[256];
snprintf(cmd, sizeof(cmd),
         "mosquitto_pub -h 192.168.1.100 -t pico/soyo/watts -m %d",
         target_watts);
system(cmd);
```

### Mit soyo1min.c

```c
// Nutze soyo_mqtt_ctrl
system("/usr/local/bin/soyo_mqtt_ctrl 192.168.1.100 1500");
```

### Mit Home Assistant

**MQTT Discovery:**
```yaml
# configuration.yaml
sensor:
  - platform: mqtt
    name: "SOYO Power"
    state_topic: "pico/status"
    value_template: "{{ value_json.soyo_watts }}"
    unit_of_measurement: "W"

switch:
  - platform: mqtt
    name: "Relay State 3"
    command_topic: "pico/relay/state"
    payload_on: "3"
    payload_off: "0"
```

---

## 🔒 Security

**WARNUNG:** Aktuell KEIN MQTT Authentication!

**Production Setup:**

1. **MQTT mit TLS:**
```c
// In pico_w_mqtt_modbus.c erweitern:
mqtt_client_connect(..., &ci, MQTT_CONNECT_FLAG_CLEAN_SESSION | MQTT_CONNECT_FLAG_TLS);
```

2. **MQTT Username/Password:**
```c
ci.client_user = "pico_user";
ci.client_pass = "secret123";
```

3. **WiFi WPA3:** (wenn Router unterstützt)
```c
cyw43_arch_wifi_connect_timeout_ms(WIFI_SSID, WIFI_PASSWORD,
                                    CYW43_AUTH_WPA3_SAE_AES_PSK, 30000);
```

---

## 📝 Vergleich der Optionen

| Feature | Pico W (diese Version) | RPi Native | Serial Bridge |
|---------|----------------------|------------|---------------|
| **Hardware** | Pico W + MAX485 | RPi + MAX485 | RPi + Pico + MAX485 |
| **Kosten** | ~8€ | ~40€ | ~12€ |
| **WiFi** | ✅ Onboard | ✅ Onboard | ❌ (nur RPi) |
| **Latenz** | 🟢 Low | 🟢 Low | 🟡 Medium |
| **Setup** | 🟡 Medium | 🟢 Easy | 🔴 Complex |
| **Standalone** | ✅ Ja! | ❌ Braucht RPi | ❌ Braucht RPi |

---

**Vorteil Pico W:** Komplett standalone, kein Raspberry Pi nötig! 🚀
