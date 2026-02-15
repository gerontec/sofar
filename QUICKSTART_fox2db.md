# 🚀 fox2db Quick Start Guide

Schnellstart-Anleitung für die C-Version von `fox2db`.

---

## ⚡ 5-Minuten-Installation (Raspberry Pi)

### 1. Dependencies installieren

```bash
sudo apt update
sudo apt install -y build-essential libpaho-mqtt-dev libcjson-dev
```

### 2. Projekt kompilieren

```bash
cd /home/pi/sofar

# Automatische Installation
chmod +x install_fox2db.sh
./install_fox2db.sh

# ODER manuell:
make
```

### 3. Testen

```bash
# Version prüfen
./fox2db --version
# Output: fox2db v1.50-C

# Hilfe anzeigen
./fox2db --help

# Test-Suite ausführen
chmod +x test_fox2db.sh
./test_fox2db.sh
```

---

## 🎯 Erste Ausführung

### Variante A: Mit Defaults

```bash
./fox2db
```

**Voraussetzungen:**
- MQTT Broker auf `kellertreppe.fritz.box:1883` erreichbar
- Topic `inverter/power_grid_exchange/json` vorhanden
- Python-Skripte in `/home/pi/python/`

### Variante B: Eigene Config

```bash
# Erstelle eigenes Config-Script
cp fox2db_example.sh my_config.sh

# Passe Werte an
nano my_config.sh
```

**Beispiel-Inhalt:**
```bash
#!/bin/bash
./fox2db \
  --mqtt-broker "192.168.178.50" \
  --mqtt-port 1883 \
  --min-excess 1200 \
  --max-soc 95
```

**Ausführen:**
```bash
chmod +x my_config.sh
./my_config.sh
```

---

## 📊 Log-Monitoring

### Echtzeit-Log

```bash
tail -f /run/user/1000/fox2db.log
```

**Ausgabe:**
```
[2026-02-15 14:23:45 v1.50-C] --- Start Cycle ---
[2026-02-15 14:23:45 v1.50-C] MQTT Connected to kellertreppe.fritz.box
[2026-02-15 14:23:46 v1.50-C] MQTT Received: PCC=1250W, Bat1=-150W, SOC_Bat1=87.3%
[2026-02-15 14:23:46 v1.50-C] Data: SOC2=45.2% SOC1=87.3% PCC=1250W Bat1=-150W EBox=2650W (State=3) Stable=5
[2026-02-15 14:23:46 v1.50-C] Result: State 3 (TRACE: POWER_MATCHING (Excess: 3750W, Budget: 5250W))
[2026-02-15 14:23:47 v1.50-C] Relay OK: 3->3 | Success
```

### Wichtige Log-Meldungen

| Meldung | Bedeutung | Aktion |
|---------|-----------|--------|
| `MQTT Connected` | MQTT OK | ✅ Normal |
| `MQTT Timeout` | MQTT-Fehler | ⚠️ Broker prüfen |
| `EMERGENCY SHUTDOWN` | Kritischer Fehler | 🚨 Sofort prüfen! |
| `DEEP_DISCHARGE_PROTECTION ACTIVATED` | SOC zu niedrig | ⚠️ Batterie wird geladen |
| `Relay OK` | State-Wechsel erfolgreich | ✅ Normal |
| `Relay ERROR` | Relay-Fehler | ⚠️ EByte-Script prüfen |

---

## 🔧 Häufige Konfigurationen

### 1. Aggressivere Regelung

```bash
./fox2db \
  --min-excess 800 \
  --max-grid-draw 2000 \
  --hysteresis 300
```

**Effekt:**
- Schaltet früher hoch (800W statt 1010W)
- Erlaubt mehr Netzbezug (2000W statt 1500W)
- Reagiert schneller (300W statt 505W Hysterese)

### 2. Konservativere Regelung

```bash
./fox2db \
  --min-excess 1500 \
  --max-grid-draw 1000 \
  --hysteresis 800 \
  --stabilization-cycles 5
```

**Effekt:**
- Schaltet später hoch (1500W)
- Weniger Netzbezug (1000W)
- Mehr Stabilität (5 Zyklen = 2,5 Minuten)

### 3. Batterie-Schutz-Modus

```bash
./fox2db \
  --deep-discharge-lower 10 \
  --deep-discharge-upper 15 \
  --deep-discharge-target 12 \
  --bat-discharge-threshold -100
```

**Effekt:**
- Schutz aktiviert bei 10% SOC
- Deaktiviert erst bei 15% SOC (große Hysterese!)
- Lädt bis 12% während Schutz aktiv
- Verhindert Hochschalten bei Batterie-Entladung > 100W

---

## 🤖 Automatisierung

### Cron-Job (alle 30 Sekunden)

```bash
# Crontab bearbeiten
crontab -e

# Folgende Zeilen hinzufügen:
* * * * * /home/pi/sofar/fox2db >> /run/user/1000/fox2db_cron.log 2>&1
* * * * * sleep 30; /home/pi/sofar/fox2db >> /run/user/1000/fox2db_cron.log 2>&1
```

**Prüfen:**
```bash
# Crontab anzeigen
crontab -l

# Log prüfen
tail -f /run/user/1000/fox2db_cron.log
```

### Systemd-Service

**Erstelle Service-Datei:**
```bash
sudo nano /etc/systemd/system/fox2db.service
```

**Inhalt:**
```ini
[Unit]
Description=SOYO Inverter Power Controller
After=network.target mosquitto.service

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/sofar
ExecStart=/home/pi/sofar/fox2db
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

**Aktivieren:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable fox2db.service
sudo systemctl start fox2db.service

# Status prüfen
sudo systemctl status fox2db.service
```

---

## 🐛 Troubleshooting

### Problem: "MQTT Timeout"

**Ursache:**
- MQTT-Broker nicht erreichbar
- Falscher Hostname/Port
- Firewall blockiert Port 1883

**Lösung:**
```bash
# Broker-Verbindung testen
mosquitto_sub -h kellertreppe.fritz.box -p 1883 -t '#' -v

# Falls nicht erreichbar, Hostname/IP prüfen:
ping kellertreppe.fritz.box

# Broker-Status prüfen (falls lokal):
sudo systemctl status mosquitto
```

### Problem: "Relay ERROR"

**Ursache:**
- EByte-Script nicht ausführbar
- Falscher Pfad
- Hardware-Fehler

**Lösung:**
```bash
# Script-Pfad prüfen
ls -la /home/pi/python/ebyteserrequest.py

# Manuell testen
python3 /home/pi/python/ebyteserrequest.py 0

# Eigenen Pfad angeben
./fox2db --ebyte-script "/opt/mein/script.py"
```

### Problem: "EBox data file not found"

**Ursache:**
- EBox-Script läuft nicht
- Falscher Pfad

**Lösung:**
```bash
# EBox-Script manuell ausführen
python3 /home/pi/python/ebox1arg.py pwr 1 > /run/user/1000/ebox15k.txt

# Prüfe Datei
cat /run/user/1000/ebox15k.txt

# Eigenen Pfad angeben
./fox2db --ebox-script "/opt/mein/ebox.py"
```

### Problem: "deep_discharge_protection_active.txt" fehlt

**Ursache:**
- Datei wurde noch nie erstellt

**Lösung:**
```bash
# Datei manuell erstellen (0 = deaktiviert)
echo "0" > /run/user/1000/deep_discharge_protection_active.txt

# Oder: fox2db erstellt sie beim ersten Durchlauf automatisch
```

---

## 📈 Performance-Vergleich

| Metrik | Python | C |
|--------|--------|---|
| **Startup** | ~200ms | ~5ms |
| **RAM** | ~25 MB | ~2 MB |
| **CPU** | 15-20% | 1-2% |
| **Binary Size** | - | ~60 KB |

**Vorteil C:**
- ✅ 40x schnellerer Start
- ✅ 12x weniger RAM
- ✅ 10x weniger CPU-Last
- ✅ Keine Python-Runtime nötig

---

## 🔐 Sicherheit

### Log-Rotation

```bash
# Verhindere zu große Log-Dateien
# In Crontab einfügen:
0 0 * * * find /run/user/1000 -name "*.log" -size +10M -delete
```

### Permissions

```bash
# Log-Verzeichnis nur für User lesbar
chmod 700 /run/user/1000

# Binary nur für User ausführbar
chmod 700 /home/pi/sofar/fox2db
```

---

## 📚 Weiterführende Dokumentation

- **Vollständige Dokumentation:** `README_fox2db.md`
- **Originalprojekt:** `fox2db.py`
- **Makefile-Targets:** `make help`
- **Test-Suite:** `./test_fox2db.sh`

---

## ✅ Checkliste für Produktiv-Einsatz

- [ ] Dependencies installiert (`libpaho-mqtt-dev`, `libcjson-dev`)
- [ ] Binary kompiliert (`make`)
- [ ] Tests erfolgreich (`./test_fox2db.sh`)
- [ ] MQTT-Broker erreichbar (`mosquitto_sub -h ... -t '#'`)
- [ ] Python-Skripte vorhanden (`ebox1arg.py`, `ebyteserrequest.py`, `fox2mqtt.py`)
- [ ] Eigene Config erstellt (`my_config.sh`)
- [ ] Log-Verzeichnis beschreibbar (`/run/user/1000`)
- [ ] Manueller Test erfolgreich (`./my_config.sh`)
- [ ] Log-Monitoring aktiv (`tail -f /run/user/1000/fox2db.log`)
- [ ] Cron-Job eingerichtet ODER Systemd-Service aktiv
- [ ] Log-Rotation konfiguriert (optional)

---

**Stand:** 2026-02-15
**Version:** v1.50-C
