# MQTT Control Utilities für Pico Modbus

**Command-line Tools zur Steuerung via MQTT**

---

## 📦 Übersicht

Zwei kleine C-Programme die SOYO und Relais über MQTT steuern:

- **`soyo_mqtt_ctrl`** - SOYO Watt-Vorgabe setzen
- **`relay_mqtt_ctrl`** - Relais-Zustand setzen

---

## 🔧 Installation

### Abhängigkeiten installieren
```bash
# Ubuntu/Debian/Raspberry Pi OS
sudo apt update
sudo apt install -y libpaho-mqtt-dev mosquitto mosquitto-clients
```

### Kompilieren
```bash
cd /path/to/sofar
make -f Makefile.mqtt_ctrl
```

**Binaries:**
- `./soyo_mqtt_ctrl`
- `./relay_mqtt_ctrl`

### System-Installation
```bash
sudo make -f Makefile.mqtt_ctrl install
# Installiert nach /usr/local/bin/
```

---

## 🚀 Nutzung

### SOYO Steuerung

**Syntax:**
```bash
soyo_mqtt_ctrl <mqtt_host> <watts> [on|off]
```

**Beispiele:**
```bash
# Lokal (MQTT Broker auf selber Maschine)
soyo_mqtt_ctrl localhost 1500        # 1500W, enable

# Remote (MQTT Broker auf anderem Gerät)
soyo_mqtt_ctrl 192.168.1.100 2000    # 2000W, enable

# Disable
soyo_mqtt_ctrl localhost 0 off       # SOYO ausschalten
```

**MQTT Topics:**
- `pico/soyo/watts` → Watt-Vorgabe (0-65535)
- `pico/soyo/enable` → Enable/Disable (0/1)

---

### Relais Steuerung

**Syntax:**
```bash
relay_mqtt_ctrl <mqtt_host> <state>
```

**Beispiele:**
```bash
relay_mqtt_ctrl localhost 0          # All OFF
relay_mqtt_ctrl localhost 3          # R1+R2 ON
relay_mqtt_ctrl localhost 7          # All ON
relay_mqtt_ctrl 192.168.1.100 11     # State 11 (remote)
```

**MQTT Topics:**
- `pico/relay/state` → Zustand (0-7, 11)
- `pico/relay/trigger` → Trigger (1 = update jetzt)

---

## 🔌 Architektur

### Variante A: Raspberry Pi + Pico W (WiFi)

```
┌─────────────────┐
│  fox2db.c       │  Steuerprogramm auf Raspberry Pi
│  soyo1min.c     │
└────────┬────────┘
         │ MQTT publish
         ↓
┌─────────────────┐
│ Mosquitto       │  MQTT Broker (Raspberry Pi)
│ localhost:1883  │
└────────┬────────┘
         │ WiFi
         ↓
┌─────────────────┐
│ Pico W          │  MQTT subscribe → g_public_ram
│ + WiFi          │  → UART0 (SOYO) + UART1 (Relais)
└─────────────────┘
```

**Vorteile:**
- ✅ Wireless
- ✅ Einfaches Protokoll (MQTT)
- ✅ Mehrere Clients möglich

**Nachteile:**
- ❌ Pico W benötigt (teurer)
- ❌ WiFi-Setup nötig

---

### Variante B: Raspberry Pi + Pico (ohne WiFi) + MQTT-zu-Serial Bridge

```
┌─────────────────┐
│  fox2db.c       │  Steuerprogramm
└────────┬────────┘
         │ MQTT publish
         ↓
┌─────────────────┐
│ Mosquitto       │  MQTT Broker
└────────┬────────┘
         │ MQTT subscribe
         ↓
┌─────────────────┐
│ mqtt_to_serial  │  Bridge-Daemon (Raspberry Pi)
│ (Daemon)        │  Hört auf MQTT → schreibt Serial
└────────┬────────┘
         │ USB Serial (/dev/ttyACM0)
         ↓
┌─────────────────┐
│ Pico            │  Empfängt Commands → g_public_ram
│ (standard)      │  → UART0 (SOYO) + UART1 (Relais)
└─────────────────┘
```

**Vorteile:**
- ✅ Standard Pico (günstiger)
- ✅ Bestehender Serial-Code nutzbar

**Nachteile:**
- ❌ Bridge-Daemon nötig

---

## 📡 MQTT Topics (Übersicht)

| Topic | Direction | Type | Beschreibung |
|-------|-----------|------|--------------|
| `pico/soyo/watts` | → Pico | `uint16_t` | Ziel-Leistung in Watt |
| `pico/soyo/enable` | → Pico | `0/1` | SOYO aktivieren |
| `pico/relay/state` | → Pico | `0-7,11` | Relais-Zustand |
| `pico/relay/trigger` | → Pico | `1` | Update-Trigger |
| `pico/status/soyo` | ← Pico | JSON | SOYO Status |
| `pico/status/relay` | ← Pico | JSON | Relais Status |

---

## 🧪 Testen

### 1. MQTT Broker starten
```bash
# Start Mosquitto
sudo systemctl start mosquitto

# Test ob erreichbar
mosquitto_pub -h localhost -t "test" -m "hello"
```

### 2. MQTT Traffic monitoren
```bash
# In einem Terminal
mosquitto_sub -h localhost -t "pico/#" -v
```

### 3. Commands senden
```bash
# In anderem Terminal
./soyo_mqtt_ctrl localhost 1500
./relay_mqtt_ctrl localhost 3
```

### 4. Output im Monitor
```
pico/soyo/watts 1500
pico/soyo/enable 1
pico/relay/state 3
pico/relay/trigger 1
```

---

## 🔨 Integration in `fox2db.c`

**Statt Serial:**
```c
// Alt (Serial)
system("/usr/local/bin/soyo_ctrl 1500");
```

**Neu (MQTT):**
```c
// Mit MQTT
char cmd[256];
snprintf(cmd, sizeof(cmd), "/usr/local/bin/soyo_mqtt_ctrl %s %d",
         mqtt_host, target_watts);
system(cmd);
```

**ODER direkt MQTT-Library nutzen:**
```c
#include <MQTTClient.h>

void set_soyo_mqtt(const char* host, int watts) {
    MQTTClient client;
    char address[256];
    snprintf(address, sizeof(address), "tcp://%s:1883", host);

    MQTTClient_create(&client, address, "fox2db",
                      MQTTCLIENT_PERSISTENCE_NONE, NULL);

    MQTTClient_connectOptions opts = MQTTClient_connectOptions_initializer;
    MQTTClient_connect(client, &opts);

    char payload[32];
    snprintf(payload, sizeof(payload), "%d", watts);
    MQTTClient_publish(client, "pico/soyo/watts", strlen(payload), payload, 1, 1, NULL);

    MQTTClient_disconnect(client, 1000);
    MQTTClient_destroy(&client);
}
```

---

## 🌐 MQTT Broker Konfiguration

**Standard Config:** `/etc/mosquitto/mosquitto.conf`

```conf
# Allow anonymous connections (für Testing)
listener 1883
allow_anonymous true

# Persistence
persistence true
persistence_location /var/lib/mosquitto/

# Logging
log_dest file /var/log/mosquitto/mosquitto.log
log_type all
```

**Restart nach Änderung:**
```bash
sudo systemctl restart mosquitto
```

---

## 📊 Performance

| Methode | Latenz | Overhead | Komplexität |
|---------|--------|----------|-------------|
| **Serial** | ~10ms | Gering | Einfach |
| **MQTT (lokal)** | ~20ms | Mittel | Mittel |
| **MQTT (WiFi)** | ~50ms | Hoch | Komplex |

**Empfehlung:**
- **Serial** für lokale Installation (Pico direkt am Raspberry Pi)
- **MQTT** für verteilte Systeme (mehrere Clients)

---

## 🐛 Troubleshooting

### "Failed to connect to MQTT broker"
```bash
# Prüfe ob Mosquitto läuft
sudo systemctl status mosquitto

# Starte Mosquitto
sudo systemctl start mosquitto

# Test Verbindung
mosquitto_pub -h localhost -t test -m hello
```

### "Failed to create MQTT client"
```bash
# Installiere Library
sudo apt install libpaho-mqtt-dev

# Recompile
make -f Makefile.mqtt_ctrl clean all
```

### "No response from Pico"
```bash
# Prüfe ob Pico MQTT topics subscribed
mosquitto_sub -h localhost -t "pico/#" -v

# Send test message
mosquitto_pub -h localhost -t "pico/soyo/watts" -m "1000"
```

---

## 📝 Nächste Schritte

1. **Für Pico W:** MQTT Client in `pico_modbus_dual.c` integrieren
2. **Für Pico (standard):** MQTT-zu-Serial Bridge schreiben
3. **Status Publishing:** Pico sendet Status via MQTT zurück

**Soll ich einen MQTT-zu-Serial Bridge Daemon schreiben?**

---

**Repository:** github.com/gerontec/sofar
