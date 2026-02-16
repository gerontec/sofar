# Pi Pico / Pico W - Modbus MQTT Controller

**Standalone und Raspberry Pi Modbus/MQTT Lösungen für SOYO Inverter und Relay Boards**

---

## 📦 Übersicht

Dieser Ordner enthält **3 verschiedene Implementierungen** für Modbus-basierte Steuerung:

| Variante | Hardware | Kosten | Standalone | Empfohlen |
|----------|----------|--------|------------|-----------|
| **[Pico W WiFi](#pico-w-wifi-mqtt)** | Pico W + 2× MAX485 | ~8€ | ✅ Ja | ⭐⭐⭐⭐⭐ |
| **[Pico Serial](#pico-serial)** | Pico + 2× MAX485 | ~5€ | ❌ Braucht PC | ⭐⭐⭐ |
| **[RPi Native](#raspberry-pi-native)** | Raspberry Pi + 2× MAX485 | ~40€ | ✅ Ja | ⭐⭐⭐⭐ |

---

## 🎯 Pico W WiFi + MQTT

**📁 Files:**
- `pico_w_mqtt_modbus.c` - Main Code
- `pico_w_mqtt_modbus_CMakeLists.txt` - Build Config
- `PICO_W_README.md` - Vollständige Dokumentation

**Architektur:**
```
MQTT Broker → WiFi → Pico W → MAX485 → SOYO/Relay
```

**Features:**
- ✅ WiFi onboard (CYW43)
- ✅ MQTT Client (subscribe/publish)
- ✅ Dual-Core (SOYO 3s + Relay event)
- ✅ Vollständig standalone!

**Hardware:**
```
Pico W GPIO 0/1 + GPIO 2 → MAX485 #1 → SOYO (RS485)
Pico W GPIO 4/5 + GPIO 6 → MAX485 #2 → Relay (RS485)
```

**Setup:**
1. Edit WiFi credentials in `pico_w_mqtt_modbus.c`
2. `cmake .. && make`
3. Flash `.uf2` to Pico W
4. Send MQTT: `mosquitto_pub -t "pico/soyo/watts" -m "1500"`

**📖 [Vollständige Anleitung →](PICO_W_README.md)**

---

## 🔌 Pico Serial

**📁 Files:**
- `pico_modbus_dual.c` - Main Code
- `pico_modbus_dual_CMakeLists.txt` - Build Config
- `PICO_MODBUS_README.md` - Dokumentation

**Architektur:**
```
USB Serial → Pico → MAX485 → SOYO/Relay
```

**Features:**
- ✅ Dual-Core (SOYO 3s + Relay event)
- ✅ USB Serial Console
- ✅ Simple Commands (`s 1500`, `r 3`, `q`)
- ❌ Braucht PC/RPi für Serial Connection

**Hardware:**
```
Pico GPIO 0/1 + GPIO 2 → MAX485 #1 → SOYO
Pico GPIO 4/5 + GPIO 6 → MAX485 #2 → Relay
USB Cable → PC/RPi
```

**Setup:**
1. `cmake .. && make`
2. Flash `.uf2`
3. `screen /dev/ttyACM0 115200`
4. Commands: `s 1500`, `r 3`, `q`

**📖 [Vollständige Anleitung →](PICO_MODBUS_README.md)**

---

## 🖥️ Raspberry Pi Native

**📁 Files:**
- `rpi_modbus_mqtt_daemon.c` - Daemon Code
- `pico_mqtt_daemon.c` - MQTT-zu-Serial Bridge (optional)
- `Makefile.mqtt_ctrl` - Build System

**Architektur:**

**Option A: Direct (empfohlen):**
```
MQTT → RPi Daemon → GPIO UARTs → MAX485 → SOYO/Relay
```

**Option B: Mit Pico als Bridge:**
```
MQTT → RPi Daemon → USB Serial → Pico → MAX485 → SOYO/Relay
```

**Features:**
- ✅ Linux (einfaches Debugging)
- ✅ MQTT Client
- ✅ Systemd Service
- ✅ Auto-restart

**Hardware (Option A):**
```
RPi GPIO 14/15 (UART0) + GPIO 2 → MAX485 #1 → SOYO
RPi GPIO 0/1 (UART2) + GPIO 3 → MAX485 #2 → Relay
```

**Setup:**
```bash
make -f Makefile.mqtt_ctrl
sudo make -f Makefile.mqtt_ctrl install
sudo systemctl start pico-mqtt-daemon
```

**📖 [Daemon Anleitung →](DAEMON_README.md)**

---

## 🛠️ MQTT Control Tools

**Command-line Tools für alle Varianten:**

**📁 Files:**
- `soyo_mqtt_ctrl.c` - SOYO via MQTT
- `relay_mqtt_ctrl.c` - Relay via MQTT
- `soyo_ctrl.c` - SOYO via Serial
- `relay_ctrl.c` - Relay via Serial

**Build:**
```bash
# MQTT Tools (benötigt libpaho-mqtt-dev)
make -f Makefile.mqtt_ctrl

# Serial Tools (keine Dependencies)
make -f Makefile.pico_ctrl
```

**Usage:**
```bash
# MQTT
soyo_mqtt_ctrl localhost 1500      # Set 1500W
relay_mqtt_ctrl localhost 3        # Relay state 3

# Serial (direkt an Pico)
soyo_ctrl 1500                     # Set 1500W
relay_ctrl 3                       # Relay state 3
```

**📖 [MQTT Tools Doku →](MQTT_CTRL_README.md)**

---

## 📊 Vergleich

| Feature | Pico W | Pico Serial | RPi Native |
|---------|--------|-------------|------------|
| **Kosten** | ~8€ | ~5€ | ~40€ |
| **Power** | 0.5W | 0.5W | 5W |
| **WiFi** | ✅ Onboard | ❌ | ✅ |
| **Standalone** | ✅ | ❌ | ✅ |
| **Setup** | Medium | Easy | Easy |
| **Debugging** | Medium | Easy | Very Easy |
| **MQTT** | ✅ Direct | ❌ | ✅ |
| **USB Serial** | ✅ | ✅ | ❌ |

**Empfehlung:**
- **Production:** Pico W (standalone, günstig)
- **Development:** Pico Serial (einfaches Debugging)
- **Complex Setup:** RPi Native (mehr Power, Linux)

---

## 📁 File Übersicht

### Pico W (WiFi + MQTT)
```
pico_w_mqtt_modbus.c                 - Main code
pico_w_mqtt_modbus_CMakeLists.txt    - Build config
PICO_W_README.md                      - Documentation
```

### Pico (Serial)
```
pico_modbus_dual.c                    - Main code
pico_modbus_dual_CMakeLists.txt       - Build config
PICO_MODBUS_README.md                 - Documentation
PICO_MEMORY_MAP.md                    - Memory layout
```

### Raspberry Pi
```
rpi_modbus_mqtt_daemon.c              - Direct GPIO version
pico_mqtt_daemon.c                    - Serial bridge version
pico-mqtt-daemon.service              - Systemd service
DAEMON_README.md                      - Daemon documentation
```

### Control Tools
```
soyo_mqtt_ctrl.c                      - SOYO MQTT control
relay_mqtt_ctrl.c                     - Relay MQTT control
soyo_ctrl.c                           - SOYO serial control
relay_ctrl.c                          - Relay serial control
MQTT_CTRL_README.md                   - Tools documentation
```

### Build Systems
```
Makefile.mqtt_ctrl                    - MQTT tools + daemon
Makefile.pico_ctrl                    - Serial tools
```

---

## 🚀 Quick Start

### 1. Pico W (empfohlen)

```bash
# Edit WiFi credentials
vim pico_w_mqtt_modbus.c  # Line 55-57

# Build
export PICO_SDK_PATH=$HOME/pico-sdk
mkdir build && cd build
cmake -f pico_w_mqtt_modbus_CMakeLists.txt ..
make -j4

# Flash
# Hold BOOTSEL → Copy pico_w_mqtt_modbus.uf2 → Done!

# Use
mosquitto_pub -t "pico/soyo/watts" -m "1500"
```

### 2. Pico Serial

```bash
# Build
mkdir build && cd build
cmake -f pico_modbus_dual_CMakeLists.txt ..
make -j4

# Flash & Connect
# Hold BOOTSEL → Copy pico_modbus_dual.uf2
screen /dev/ttyACM0 115200

# Use
> s 1500
> r 3
> q
```

### 3. Raspberry Pi

```bash
# Install
sudo apt install libpaho-mqtt-dev mosquitto
make -f Makefile.mqtt_ctrl
sudo make -f Makefile.mqtt_ctrl install-daemon

# Start
sudo systemctl start pico-mqtt-daemon

# Use
mosquitto_pub -t "pico/soyo/watts" -m "1500"
```

---

## 🔧 Hardware Requirements

### All Variants Need:
- **2× MAX485 Module** (~1€ each)
  - Converts TTL UART ↔ RS485
  - Pins: VCC, GND, DI, RO, DE, RE, A, B

### Variant-Specific:
- **Pico W:** Raspberry Pi Pico W (~6€)
- **Pico:** Raspberry Pi Pico (~4€)
- **RPi:** Raspberry Pi 4/5 (~40€)

### RS485 Devices:
- **SOYO Inverter** (4800 baud, Modbus RTU)
- **Relay Board** (9600 baud, Modbus RTU)

---

## 📖 Documentation Index

| Document | Description |
|----------|-------------|
| **[PICO_W_README.md](PICO_W_README.md)** | Pico W WiFi + MQTT Guide |
| **[PICO_MODBUS_README.md](PICO_MODBUS_README.md)** | Pico Serial Guide |
| **[PICO_MEMORY_MAP.md](PICO_MEMORY_MAP.md)** | Memory Layout Reference |
| **[DAEMON_README.md](DAEMON_README.md)** | RPi Daemon Guide |
| **[MQTT_CTRL_README.md](MQTT_CTRL_README.md)** | MQTT Tools Guide |

---

## 🤝 Integration

### Mit fox2db.c / soyo1min.c

**Via MQTT:**
```c
char cmd[256];
snprintf(cmd, sizeof(cmd),
         "mosquitto_pub -h localhost -t pico/soyo/watts -m %d",
         target_watts);
system(cmd);
```

**Via Serial (wenn Pico verbunden):**
```c
FILE *fp = fopen("/dev/ttyACM0", "w");
fprintf(fp, "s %d\n", target_watts);
fclose(fp);
```

---

## 🐛 Troubleshooting

### Pico W WiFi failed
- ✓ SSID/Password korrekt?
- ✓ 2.4GHz WiFi? (Pico W unterstützt kein 5GHz!)
- ✓ In Reichweite?

### MQTT connection failed
- ✓ Broker läuft? (`sudo systemctl status mosquitto`)
- ✓ IP korrekt?
- ✓ Port 1883 offen?

### RS485 timeout
- ✓ MAX485 Verkabelung korrekt?
- ✓ A/B vertauscht?
- ✓ DE/RE connected?
- ✓ Device eingeschaltet?

---

## 📜 License

Same as parent project.

---

**Built for production solar control systems.** 🔋☀️
