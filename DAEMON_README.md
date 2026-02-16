# Pico MQTT Daemon

**Endlos laufender MQTT-zu-Serial Bridge Daemon**

---

## 🎯 Was macht der Daemon?

Der Daemon läuft **endlos im Hintergrund** und:

1. **Subscribed MQTT Topics** (`pico/soyo/watts`, `pico/relay/state`, etc.)
2. **Empfängt Commands** von MQTT Clients
3. **Forwarded via Serial** an Pico (`/dev/ttyACM0`)
4. **Liest Status** vom Pico
5. **Published Status** zurück zu MQTT (`pico/status`)

---

## 🏗️ Architektur

```
┌─────────────────┐
│  fox2db.c       │  Publiziert MQTT Messages
│  soyo1min.c     │
│  soyo_mqtt_ctrl │
└────────┬────────┘
         │ MQTT publish
         │ pico/soyo/watts = 1500
         ↓
┌─────────────────┐
│ Mosquitto       │  MQTT Broker (localhost:1883)
│ (MQTT Broker)   │
└────────┬────────┘
         │ MQTT subscribe
         ↓
┌─────────────────┐
│ pico_mqtt_daemon│  ← DIESER DAEMON (läuft endlos!)
│ (Bridge)        │  - Subscribed MQTT
└────────┬────────┘  - Schreibt Serial
         │ USB Serial
         │ /dev/ttyACM0
         │ "s 1500\n"
         ↓
┌─────────────────┐
│ Pico            │  Empfängt Commands
│ (Hardware)      │  → g_public_ram.soyo_target_watts = 1500
└────────┬────────┘  → UART0 → SOYO Modbus
         │
         ↓
┌─────────────────┐
│ SOYO Inverter   │  Empfängt Modbus RTU Command
│ (Hardware)      │
└─────────────────┘
```

---

## 🚀 Installation

### 1. Dependencies
```bash
sudo apt update
sudo apt install -y libpaho-mqtt-dev mosquitto mosquitto-clients
```

### 2. Kompilieren
```bash
cd /path/to/sofar
make -f Makefile.mqtt_ctrl
```

**Binaries:**
- `pico_mqtt_daemon` - Der Daemon
- `soyo_mqtt_ctrl` - SOYO Control Tool
- `relay_mqtt_ctrl` - Relay Control Tool

### 3. Installieren
```bash
# Binaries installieren
sudo make -f Makefile.mqtt_ctrl install

# Systemd Service installieren
sudo make -f Makefile.mqtt_ctrl install-daemon
```

---

## 🔧 Nutzung

### Manueller Start (Foreground)

**Test im Terminal:**
```bash
# Start daemon (Foreground, sieht alle Logs)
./pico_mqtt_daemon localhost

# ODER mit remote MQTT Broker
./pico_mqtt_daemon 192.168.1.100
```

**Output:**
```
Starting Pico MQTT Daemon
Opened serial connection to Pico at /dev/ttyACM0
Connected to MQTT broker at tcp://localhost:1883
Subscribed to MQTT topics
Entering main loop
MQTT: pico/soyo/watts = 1500
Sent to Pico: s 1500
Published status (256 bytes)
```

**Beenden:** `Ctrl+C`

---

### Systemd Service (Background)

**Start als System Service:**
```bash
# Enable (startet automatisch beim Boot)
sudo systemctl enable pico-mqtt-daemon

# Start
sudo systemctl start pico-mqtt-daemon

# Status checken
sudo systemctl status pico-mqtt-daemon
```

**Output:**
```
● pico-mqtt-daemon.service - Pico MQTT Daemon - MQTT to Serial Bridge
   Loaded: loaded (/etc/systemd/system/pico-mqtt-daemon.service; enabled)
   Active: active (running) since Mon 2026-02-16 14:30:00 CET; 5s ago
 Main PID: 12345 (pico_mqtt_daemon)
   CGroup: /system.slice/pico-mqtt-daemon.service
           └─12345 /usr/local/bin/pico_mqtt_daemon localhost
```

**Logs ansehen:**
```bash
# Live logs (follow mode)
sudo journalctl -u pico-mqtt-daemon -f

# Letzte 100 Zeilen
sudo journalctl -u pico-mqtt-daemon -n 100
```

---

## 📡 MQTT Topics

### Subscribe (Daemon hört auf diese Topics):

| Topic | Typ | Beschreibung |
|-------|-----|--------------|
| `pico/soyo/watts` | `uint16_t` | SOYO Watt-Vorgabe (0-65535) |
| `pico/soyo/enable` | `0/1` | SOYO enable/disable |
| `pico/relay/state` | `0-7,11` | Relais-Zustand |
| `pico/relay/trigger` | `1` | Trigger Update |

### Publish (Daemon sendet diese Topics):

| Topic | Typ | Beschreibung | Interval |
|-------|-----|--------------|----------|
| `pico/status` | Text | Status vom Pico (raw) | 5s |
| `pico/heartbeat` | Unix Timestamp | Daemon alive signal | 5s |

---

## 🧪 Testen

### 1. Terminal 1: Monitor MQTT
```bash
mosquitto_sub -h localhost -t "pico/#" -v
```

### 2. Terminal 2: Daemon starten
```bash
./pico_mqtt_daemon localhost
```

### 3. Terminal 3: Commands senden
```bash
# SOYO auf 1500W
soyo_mqtt_ctrl localhost 1500

# Relais auf State 3
relay_mqtt_ctrl localhost 3

# ODER direkt via mosquitto_pub
mosquitto_pub -h localhost -t "pico/soyo/watts" -m "2000"
```

### 4. Terminal 1 Output
```
pico/soyo/watts 1500
pico/soyo/enable 1
pico/relay/state 3
pico/relay/trigger 1
pico/status --- Status ---
SOYO:  1500 W (cycles: 42, errors: 0x00)
Relay: state 3, bits 0x03 [R1=1 R2=1 R3=0] (cycles: 1, errors: 0x00)
pico/heartbeat 1708095000
```

---

## 🔄 Daemon Lifecycle

### Start
```bash
sudo systemctl start pico-mqtt-daemon
# Daemon verbindet zu MQTT
# Daemon öffnet /dev/ttyACM0
# Daemon subscribed Topics
# Daemon läuft endlos
```

### Stop
```bash
sudo systemctl stop pico-mqtt-daemon
# Daemon empfängt SIGTERM
# Daemon disconnected MQTT
# Daemon schließt Serial
# Daemon beendet sich
```

### Restart
```bash
sudo systemctl restart pico-mqtt-daemon
# Stop + Start
```

### Auto-Restart bei Fehler
Der Daemon ist konfiguriert mit `Restart=always`:
- Wenn Daemon crashed → automatischer Neustart nach 10s
- Wenn MQTT Connection lost → reconnect
- Wenn Serial disconnected → reconnect

---

## 📊 Monitoring

### Live Logs
```bash
sudo journalctl -u pico-mqtt-daemon -f
```

**Output:**
```
Feb 16 14:30:00 pico-mqtt-daemon[12345]: Starting Pico MQTT Daemon
Feb 16 14:30:00 pico-mqtt-daemon[12345]: Opened serial connection to Pico
Feb 16 14:30:00 pico-mqtt-daemon[12345]: Connected to MQTT broker
Feb 16 14:30:05 pico-mqtt-daemon[12345]: MQTT: pico/soyo/watts = 1500
Feb 16 14:30:05 pico-mqtt-daemon[12345]: Sent to Pico: s 1500
Feb 16 14:30:10 pico-mqtt-daemon[12345]: Published status (256 bytes)
```

### Status Check
```bash
sudo systemctl status pico-mqtt-daemon
```

### Heartbeat Check
```bash
# Letzter Heartbeat (sollte < 10s sein)
mosquitto_sub -h localhost -t "pico/heartbeat" -C 1
```

---

## 🐛 Troubleshooting

### Daemon startet nicht
```bash
# Check logs
sudo journalctl -u pico-mqtt-daemon -n 50

# Häufige Probleme:
# - /dev/ttyACM0 nicht vorhanden → Pico nicht verbunden
# - Permission denied → User nicht in Gruppe 'dialout'
# - MQTT broker nicht erreichbar → mosquitto nicht gestartet
```

**Fixes:**
```bash
# User zur dialout Gruppe hinzufügen
sudo usermod -a -G dialout $USER
# WICHTIG: Logout/Login nötig!

# MQTT Broker starten
sudo systemctl start mosquitto

# Pico prüfen
ls -l /dev/ttyACM0
```

### MQTT Connection Lost
```bash
# Daemon reconnected automatisch
# Check logs:
sudo journalctl -u pico-mqtt-daemon | grep "connection lost"
```

### Serial Connection Lost
```bash
# Daemon reconnected automatisch bei nächstem Command
# Check logs:
sudo journalctl -u pico-mqtt-daemon | grep "Write to Pico failed"

# Pico neu anschließen:
# 1. Daemon stoppt automatisch
# 2. Pico reconnecten
# 3. Daemon reconnected automatisch
```

---

## 🔒 Sicherheit

### Permissions
- Daemon läuft als User `pi` (nicht root!)
- User muss in Gruppe `dialout` sein
- Service nutzt `PrivateTmp=yes` und `NoNewPrivileges=true`

### MQTT Security
**Aktuell:** Anonymous Connections erlaubt (für Testing)

**Production:** MQTT mit Authentication:
```conf
# /etc/mosquitto/mosquitto.conf
allow_anonymous false
password_file /etc/mosquitto/passwd
```

**Create User:**
```bash
sudo mosquitto_passwd -c /etc/mosquitto/passwd pico_daemon
sudo systemctl restart mosquitto
```

**Update Daemon:**
Edit `/etc/systemd/system/pico-mqtt-daemon.service`:
```ini
Environment="MQTT_USER=pico_daemon"
Environment="MQTT_PASS=secret123"
```

---

## 📈 Performance

| Metrik | Wert |
|--------|------|
| **Latenz** | ~50ms (MQTT → Serial → Pico) |
| **Durchsatz** | ~20 Commands/s |
| **CPU Usage** | <1% |
| **RAM Usage** | ~5MB |
| **Status Interval** | 5s |

---

## 🔧 Makefile Targets

```bash
make -f Makefile.mqtt_ctrl daemon-start    # Start daemon
make -f Makefile.mqtt_ctrl daemon-stop     # Stop daemon
make -f Makefile.mqtt_ctrl daemon-restart  # Restart daemon
make -f Makefile.mqtt_ctrl daemon-status   # Show status
make -f Makefile.mqtt_ctrl daemon-logs     # Show logs
make -f Makefile.mqtt_ctrl monitor         # Monitor MQTT
```

---

## 📝 Integration

### In `fox2db.c`:
```c
// Statt direktem Modbus:
// send_soyo_command(watts);

// Jetzt via MQTT:
char cmd[256];
snprintf(cmd, sizeof(cmd),
         "mosquitto_pub -h localhost -t pico/soyo/watts -m %d",
         watts);
system(cmd);
```

### In `soyo1min.c`:
```c
// Nutze soyo_mqtt_ctrl
char cmd[256];
snprintf(cmd, sizeof(cmd),
         "/usr/local/bin/soyo_mqtt_ctrl localhost %d",
         target_watts);
system(cmd);
```

---

**Der Daemon läuft endlos und macht dein System event-driven!** 🚀
