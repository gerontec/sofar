# fox2db Cutover — Pi (prod) → Waveshare ESP (dev)

Schrittweise Übergabe der **physischen Steuerung** von EBox/WR2 vom Raspberry Pi
(`fox2db.py` + EByte-Relayboard) auf den Waveshare ESP32-S3 (ESPHome + `fox2db_logic.h`).

> **Goldene Regel:** Zu jedem Zeitpunkt darf **nur EIN Controller** physisch an
> EBox/WR2 hängen. Cutover = Verdrahtung umstecken + Pi-Steuerung deaktivieren,
> in EINEM Wartungsfenster.

---

## 0. Architektur

| | Host | Rolle vorher (Shadow) | Rolle nachher (Cutover) |
|---|---|---|---|
| **Pi** | 192.168.178.119 | Steuert EBox/WR2 (EByte), liest BMS, schreibt DB | Shadow/Referenz: liest BMS → `ebox/status`, **steuert NICHT** |
| **ESP** | 192.168.178.189 | Shadow: rechnet, Relais unverdrahtet | **Steuert EBox/WR2** (Waveshare CH1-3=State, CH4=DO4) |
| **Broker/DB** | 192.168.178.218 | MQTT + MariaDB + Frontend + ESPHome-Dashboard | unverändert |

**MQTT-Topics:**
- `inverter/power_grid_exchange/json` → PCC/Bat1/SOC1 (beide lesen)
- `ebox/status` `{"soc":SOC2,"power":W}` → SOC2 (Pi published, ESP liest)
- `fox2db/state` → Pi-Entscheidung · `sofar/state` → ESP-Entscheidung
- Steuerung: `sofar/auto`, `sofar/ladesperre`, `waveshare/relay/N`

---

## 1. Akzeptanzkriterien (vor Cutover erfüllen)

Im Frontend (http://192.168.178.218:8088/) Vergleich Pi↔ESP beobachten. Cutover erst wenn:

- [ ] **≥ 48 h Tageslicht-Betrieb** im Shadow ohne `State`-Abweichung (Vergleichszeile durchgehend `=`)
- [ ] **Decision** (POWER_MATCHING / PCC_OVER_20KW / GUARD:*) stimmt überein
- [ ] **BATTERY_FULL_STOP** greift beim ESP bei SOC2 ≥ 100 (gleicher Zeitpunkt wie Pi)
- [ ] **DO4-Logik**: bei PCC > 20 kW zeigt `sofar/state` `"do4":1` wann der Pi DO4 pulst
- [ ] ESP-Heap stabil (`sofar/waveshare/status` `mem_free` driftet nicht gegen 0)
- [ ] WLAN/MQTT stabil (keine Reboot-Schleife im `sofar-waveshare/debug`)

DB-Abgleich der letzten 2 Tage (auf .218):
```sql
-- Abweichungen zählen: erfordert sofar/state-Logging (siehe TODO 2). Solange nur
-- visuell im Frontend vergleichen.
```

---

## 2. TODOs VOR dem Cutover (Voraussetzungen schaffen)

Diese drei Dinge müssen existieren, BEVOR der Pi die Steuerung abgibt:

### 2a. `CONTROL_ENABLED`-Schalter in fox2db.py
Damit der Pi weiter BMS liest + `ebox/status`/`fox2db/state` publiziert + DB schreibt,
aber die EByte-Relais NICHT mehr anfasst (Reverse-Shadow + Rollback-fähig).

In `fox2db.py` einbauen:
```python
CONTROL_ENABLED = True   # Cutover: auf False → Pi steuert Relais nicht mehr
```
und die zwei Schaltstellen kapseln:
```python
if changed and CONTROL_ENABLED:
    _db_relay_event("state_change", "ebox", final, trace)
    set_relay(final)
elif not changed:
    _write(PATHS['relay_state'], final)
# DO4:
if need_downward_regulation and CONTROL_ENABLED:
    pulse_do4()
```
`ebox/status`-Publish und `_db_decision_log` bleiben aktiv (laufen vor den Schaltstellen).

### 2b. Relais-Mapping verifizieren
Waveshare-Verdrahtung muss EByte 1:1 ersetzen:
| EByte (Pi) | → | Waveshare (ESP) |
|---|---|---|
| State-Bit0 | → | CH1 (GPIO1) |
| State-Bit1 | → | CH2 (GPIO2) |
| State-Bit2 | → | CH3 (GPIO41) |
| DO4 → WR2 | → | CH4 (GPIO42) |

Trockentest **vor** Verdrahtung am Echtsystem (Auto AUS):
```bash
# Jeden State 0..7 durchschalten, Relais-Klick + Bits prüfen:
for s in 0 1 2 3 4 5 6 7; do
  mosquitto_pub -h 192.168.178.218 -t waveshare/relay/1 -m "{\"V\":$((s&1))}"
  mosquitto_pub -h 192.168.178.218 -t waveshare/relay/2 -m "{\"V\":$(((s>>1)&1))}"
  mosquitto_pub -h 192.168.178.218 -t waveshare/relay/3 -m "{\"V\":$(((s>>2)&1))}"
  echo "State $s → CH1-3 gesetzt"; sleep 1
done
mosquitto_pub -h 192.168.178.218 -t waveshare/relay/all -m '{"V":0}'   # alles aus
```

### 2c. `sofar/state` → DB-Writer (damit Reporting weiterläuft)
Kleiner Subscriber auf .218, schreibt ESP-Entscheidungen nach `pv_decision_log`
(damit `decision_report.py` + Diagramme nach Cutover die ESP-Läufe sehen).
Optional zunächst — solange der Pi (Reverse-Shadow) noch in die DB schreibt,
hat man die Referenz; den ESP-Writer spätestens bei Phase C (Decommission) starten.

---

## 3. Cutover-Prozedur (Wartungsfenster)

> **Zeitpunkt:** Nachts / bei wenig PV (EBox idle, State 0, keine Ladung aktiv,
> kein DO4 nötig). Minimales Risiko. Rollback-Plan (§5) bereithalten.

1. **Frontend offen** lassen (Vergleich + sofar/state beobachten).
2. **Pi-Steuerung deaktivieren** — `CONTROL_ENABLED = False` in `/home/pi/python/fox2db.py`
   setzen. Pi liest/publiziert/loggt weiter, fasst aber die EByte-Relais nicht mehr an.
   ```bash
   ssh pi@192.168.178.119 "sed -i 's/^CONTROL_ENABLED = True/CONTROL_ENABLED = False/' /home/pi/python/fox2db.py"
   ```
3. **EByte-Relais in sicheren Zustand** (State 0, DO4 aus) — letzter Pi-Lauf mit
   CONTROL_ENABLED=True hat State 0 gesetzt, oder manuell:
   ```bash
   ssh pi@192.168.178.119 "python3 /home/pi/python/ebyte_ctrl.py 0"
   ```
4. **HARDWARE: Verdrahtung umstecken** EBox-State-Lines + DO4/WR2 von **EByte → Waveshare**
   (Pi spannungsfrei/abgeklemmt von den Aktoren). Doppelt prüfen: CH1-3=State, CH4=DO4.
5. **ESP übernimmt** — Auto ist bereits an; sicherstellen:
   ```bash
   mosquitto_pub -h 192.168.178.218 -t sofar/auto -m '{"ENABLE":1}'
   ```
6. **`ebox/status` läuft weiter?** prüfen (Pi publiziert es weiter, da nur die
   Schaltstellen deaktiviert sind):
   ```bash
   mosquitto_sub -h 192.168.178.218 -t ebox/status -C 1 -W 70
   ```
7. **Live beobachten** (mind. eine Stunde, dann ein voller Tag):
   ```bash
   mosquitto_sub -h 192.168.178.218 -t sofar/state -v
   ```
   Prüfen: EBox schaltet korrekt, SOC2-Anstieg plausibel, **PCC bleibt < 20 kW**
   (DO4 feuert wann nötig), BATTERY_FULL_STOP bei SOC2=100.

---

## 4. Post-Cutover-Verifikation (erster voller Tag)

- [ ] Mittags-Peak: PCC bleibt < 20 kW → **CH4/DO4 schaltet WR2 ab** (physisch hörbar/messbar)
- [ ] EBox lädt bei Überschuss hoch (State steigt), runter bei Trendwende
- [ ] SOC2 = 100 → State 0 (BATTERY_FULL_STOP), keine Überladung
- [ ] Reverse-Shadow: `fox2db/state` (Pi-Referenz) ≈ `sofar/state` (ESP-real) im Frontend
- [ ] ESP-Heap/WLAN stabil über den Tag

---

## 5. Rollback (falls ESP fehlsteuert)

Schnell und sicher zurück auf den Pi:
1. **ESP Auto AUS:** `mosquitto_pub -h 192.168.178.218 -t sofar/auto -m '{"ENABLE":0}'`
   → ESP hält Relais; bei Reboot ohnehin alle aus (ALWAYS_OFF).
2. **HARDWARE: Verdrahtung zurück** Waveshare → EByte.
3. **Pi-Steuerung reaktivieren:** `CONTROL_ENABLED = True`
   ```bash
   ssh pi@192.168.178.119 "sed -i 's/^CONTROL_ENABLED = False/CONTROL_ENABLED = True/' /home/pi/python/fox2db.py"
   ```
4. Nächster Cron-Lauf (≤ 1 min) übernimmt wieder. EByte-Board nie entsorgen, bis
   der ESP wochenlang stabil läuft.

---

## 6. Decommission (Phase C — erst nach Wochen stabilem ESP-Betrieb)

Wenn der ESP dauerhaft fehlerfrei steuert:
1. **`sofar/state` → DB-Writer** starten (TODO 2c) — ESP-Entscheidungen landen in
   `pv_decision_log`; `decision_report.py` + Diagramme laufen unverändert weiter.
2. **fox2db-Cron** auf reinen `ebox/status`-Publisher reduzieren ODER eigenständiges
   `ebox_publish.py` (nur BMS-Read + Publish, keine Entscheidung) als Cron, dann
   fox2db ganz aus dem Cron nehmen:
   ```bash
   ssh pi@192.168.178.119 "crontab -l"   # ser2.py/fox2db-Zeile prüfen/entfernen
   ```
3. EByte-Board als Kalt-Reserve aufheben (nicht entsorgen).
4. ESP-Config: `ladesperre_enable` produktiv setzen, sobald Forecast validiert
   (`mosquitto_pub -t sofar/ladesperre -m '{"ENABLE":1}'`), OTA-Update via gehostetem
   Image: `esphome upload sofar_waveshare.yaml --device 192.168.178.189`.

---

## 7. Referenz — wer macht was, vorher/nachher

| Funktion | Shadow (jetzt) | nach Cutover | Decommission |
|---|---|---|---|
| EBox/WR2 schalten | Pi (EByte) | **ESP (Waveshare)** | ESP |
| BMS lesen → `ebox/status` | Pi (fox2db) | Pi (fox2db) | Pi (`ebox_publish.py`) |
| Entscheidung publizieren | beide | beide (Pi=Referenz) | ESP |
| DB `pv_decision_log` | Pi | Pi (Referenz) | sofar→DB-Writer |
| fox2db `CONTROL_ENABLED` | `True` | **`False`** | (Cron entfernt) |

---

## Wichtige Adressen
- Frontend / Vergleich: http://192.168.178.218:8088/
- ESPHome-Dashboard: http://192.168.178.218:6052/
- OTA-Image: http://192.168.178.218/firmware/sofar-waveshare.ota.bin
- ESP-Log: MQTT `sofar-waveshare/debug`
- ESP-IP: 192.168.178.189 · Pi: 192.168.178.119 · Broker/DB: 192.168.178.218
