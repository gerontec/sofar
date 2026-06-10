# Waveshare ESP32-S3-Relay-6CH — Produktionsdokumentation

**Firmware:** fox2db v3.3.28  
**Gerät:** 192.168.178.187  
**MQTT-Broker:** 192.168.178.218:1883

---

## Hardware

```
Waveshare ESP32-S3 6CH Relay
  CH1 (GPIO1)  = EBox-State Bit0 ┐
  CH2 (GPIO2)  = EBox-State Bit1 ├─ Ladezustand 0..7 binär
  CH3 (GPIO41) = EBox-State Bit2 ┘
  CH4 (GPIO42) = DO4 → WR2 abregeln (3s-Puls bei PCC>20kW)
  CH5 (GPIO45) = frei
  CH6 (GPIO46) = frei

RS485 (SP3485-Transceiver)
  GPIO17 (TX) → DI → A+/B−  (Soyo-Frame TX, alle 3s)
  GPIO18 (RX) ← RO ← A+/B−  (Frame-Empfang, 25ms Gap-Erkennung)
  GPIO3       → DE/RE         (TX-Aktivierungssteuerung via 74HC04D-Inverter)
```

---

## Firmware-Architektur — Scheduling

Die Zyklen werden **nicht** von einem RTOS-Timer pro Aufgabe getaktet, sondern vom
**ESPHome-Software-Scheduler** in einer einzigen kooperativen Hauptschleife:

```
FreeRTOS loopTask (Arduino-Core, ESP32-S3)
  └─ Arduino loop() → App.loop()        (ESPHome-Hauptschleife, läuft so schnell wie möglich)
       └─ esphome::Scheduler (millis-basiert)
            └─ interval:-Komponenten   ← hier liegen ALLE Zyklen
```

Framework ist `arduino` (`esp32: framework: type: arduino`): ein einzelner
FreeRTOS-`loopTask` führt `loop()` → `App.loop()` aus. Jedes `interval:` ist via
`App.scheduler.set_interval()` registriert und wird **jede** Schleifeniteration gegen
`millis()` geprüft. **Kooperativ — kein Preemption, kein eigener Task pro Interval.**

| Intervall | Aufgabe |
|---:|---|
| **60 s** | **Entscheidungs-Zyklus → `fox::step()`** (der eigentliche Reglertakt) |
| 60 s | Soyo-Kalkulation → `soyo/calc` |
| 50 ms | RS485: Soyo-TX (alle 3 s) + Frame-Empfang (25 ms Gap) |
| 30 s | Board-Telemetrie (`pub_status`) |
| 1 ms | GPIO-Edge-Scan (nur on-demand aktiv) |

**Konsequenzen:**
- Eine lange `lambda` (z. B. die NOAA-Sonnenstands-Rechnung im 60-s-Block) blockiert
  *alle* anderen Intervalle, bis sie fertig ist → schwere Rechnung bewusst nur 1×/60 s;
  der zeitkritische Soyo-Keepalive liegt im schlanken 50-ms-Interval (max. 50 ms Jitter,
  Soyo-Timeout 4 s).
- Timing-Basis ist `millis()` **ab Boot**, nicht auf die volle Minute synchronisiert.
  SNTP (`id: sntp_time`) liefert nur die Wanduhr *innerhalb* des Lambdas
  (Stunde/Monat/Tag für DC-Forecast & Ladesperre-Fenster) — es taktet den Zyklus nicht.
- Frische-Check ebenfalls `millis()`-basiert: WR-Daten > 3 min alt → Zwangs-State 0.

---

## Ladelogik — fox2db (EBox2-Steuerung via CH1–CH3)

### State-Tabelle

| State (Bitmask) | Leistung | CH1 | CH2 | CH3 |
|:-:|---:|:-:|:-:|:-:|
| 0 | 0 W    | 0 | 0 | 0 |
| 1 | 3000 W | 1 | 0 | 0 |
| 2 | 3650 W | 0 | 1 | 0 |
| 3 | 6650 W | 1 | 1 | 0 |
| 4 | 3900 W | 0 | 0 | 1 |
| 5 | 7100 W | 1 | 0 | 1 |
| 6 | 7800 W | 0 | 1 | 1 |
| 7 | 11400 W| 1 | 1 | 1 |

### Entscheidungspipeline (alle 60s)

```
decide()         → POWER_MATCHING / INSUFFICIENT_EXCESS / PCC_OVER_20KW
apply_guards()   → LADESPERRE / BATTERY_FULL / CRITICAL_SOC  (unüberwindbar)
apply_blocking() → SWEET_SPOT_HOLD / TREND_BLOCK / BAT_GUARD_BLOCK (hoch)
                   STABILIZING / HYSTERESIS                          (runter)
```

**Key-Parameter:**
- `MIN_EXCESS = 1200 W` — Mindestüberschuss für Laden
- `MAX_GRID_DRAW = 1200 W` — max. erlaubter Netzbezug beim Schalten
- `HYSTERESIS = 505 W` — Mindest-Leistungsdiff für Runterschalten
- `STABILIZATION = 2` — Zyklen stabil vor Runterschalten
- `PCC_PEAK_TH = 20000 W` — Schwelle für DO4-Trigger / LADESPERRE-Freigabe
- `PCC_HARD_TH = 22000 W` — bedingungsloser DO4-Puls

### LADESPERRE

An klaren Tagen mit vorhergesagtem >20 kW-Peak wird der Akku morgens **leer gehalten**,
damit er den Mittagspeak schluckt statt DO4-Abregelung auszulösen.

**Grundprinzip (seit v3.3.27): Sperre nur bei BELEGTEM Gutwetter — sonst OFF.**

```
in_window  = has_peak && win_end_h >= 0 && !peak_today && local_hour <= peak_h
ladesperre = in_window && ratio_ist >= 0 && ratio_ist <= ratio_th
```

`ratio_ist = (dc_expected − (pcc_avg + ebox + bat1)) / dc_expected` — Anteil der
gegenüber dem Klarhimmel-Modell fehlenden Leistung. Wird jede Minute neu berechnet
(zustandslos, **kein DB-Event**). `-1` solange nicht beurteilbar.

**Sperre AKTIV** nur wenn:
- Modell sagt heute >20 kW-Peak (`has_peak`) **und**
- aktuelle Stunde ≤ Peak-Stunde (`local_hour <= peak_h`) **und**
- PCC hat 20 kW heute noch nicht erreicht (`!peak_today`) **und**
- **gültige** Ist-Ratio ≤ Schwelle (`ratio_th`, default 0.5) → belegtes Gutwetter

**Sperre OFF** (Laden frei) sobald:
- `ratio_ist > ratio_th` → Schlechtwetter (Klarhimmel bleibt aus)
- `ratio_ist < 0` → **Wetter unbeurteilbar** (Boot, Nacht, DC<5 kW) → Default OFF
- `pcc > 20 kW` → `peak_today=true`, Peak gesehen, ab jetzt laden
- `local_hour > peak_h` → Peak-Stunde überschritten, kein 20 kW-Peak mehr möglich

Die Schwelle `ratio_th` ist zur Laufzeit per MQTT änderbar (`sofar/ratio`, default 0.5).
`peak_today` verhindert Oszillation nach Freigabe durch PCC-Abfall beim Laden.

### DC-Klarhimmel-Modell (Meinel)

Berechnet stündlichen Ertrag für LADESPERRE-Ratio und DO4-Fenster.
EBox-Leistung kommt **vorzeichenbehaftet** aus `ebox/pwr` (`power_w`, + = Laden /
− = Entladen via Soyo) — kein `abs()`, damit die Ratio bei EBox-Entladung korrekt bleibt:
- **Süd-Arrays:** Tilt 25°/Azimut +80°, 27,854 Wp; Tilt 60°/Azimut −5°, 11,138 Wp  
- **Ost-Arrays:** 3 Flächen, gesamt ~24,308 Wp  
- **Standort:** 47.6811° N, 11.5732° E  
- **kt_month:** Jan 0.33 … Jun 0.88 … Dez 0.13

### DO4-Puls (WR2-Abregelung)

Schaltet CH4 für 3s (akustischer Alarm):
- `pcc > 22 kW` bedingungslos
- `pcc > 20 kW` wenn kein weiteres Hochschalten möglich (SOC=100% oder State=7) oder LADESPERRE aktiv

---

## Entladelogik — Soyo-Inverter (RS485, CH1–CH3 unabhängig)

### Soyo-Kalkulation (alle 60s, `soyo/calc`)

Der Soyo-Wechselrichter (max. 900 W) gleicht Netzbezug aus wenn EBox **nicht** lädt:

```
if State != 0:       w = 0   (EBox lädt → Soyo aus)
elif soc2 < 9%:      w = 0   (Entladeschutz)
elif ebox > 200W:    w = 0   (bat2 lädt gerade)
elif pcc > 200W:     w = 0   (PV-Überschuss vorhanden)
elif pcc < −100W:    w = |pcc| × 1.01 + (Nacht: +468W)
else:                w = 468W (Nacht) oder 10W (Tag, Standby)
```

### RS485-Frame

```
[0x24, 0x56, 0x00, 0x21, PH, PL, 0x80, CRC]
CRC = (264 − PH − PL) & 0xFF
```

Der Frame wird **immer alle 3s** über RS485 gesendet — der Soyo-Inverter benötigt einen Keepalive (Timeout=4s). Das MQTT-Topic `soyo/sent` wird **nur bei Wertänderung** publiziert.

**Keep-Alive unterbrechungssicher (seit v3.3.x):** Die TX-Sequenz läuft auf einem
unabhängigen `millis()`-Timer, der alle 50 ms geprüft wird (max. 50 ms Jitter), und ist
vom MQTT-Publish **entkoppelt** — der RS485-Frame geht auch dann raus, wenn MQTT gerade
reconnectet, OTA läuft oder ein RS485-Scan aktiv ist. Damit kann der 4s-Timeout des Soyo
nicht mehr durch andere Tasks gerissen werden.

> ⚠️ In **früheren Versionen** war an dieser Stelle ein Bug: die Keep-Alive-Sequence
> konnte durch konkurrierende Tasks/Publishes unterbrochen werden, wodurch der Soyo nach
> 4s in Timeout lief und die Entladung abschaltete. Behoben durch den entkoppelten
> 50ms-Timer (`last_soyo_tx`), unabhängig vom `soyo/sent`-Publish.

MQTT `soyo/sent`: `{"w":900,"hex":"245600210384800F","sends":120,"changes":3}`  
MQTT `soyo/calc`: `{"W":468,"soc2":63.1,"stale":0}`

---

## MQTT-API

### Status-Topics (← ESP32, lesend)

---

#### `sofar/state` — Entscheidungslog (alle 60s)

```json
{
  "ip":          "192.168.178.187",
  "state":       1,
  "changed":     1,
  "pcc":         3410,
  "bat1":        2500,
  "soc2":        68.4,
  "soc1":        37.0,
  "ebox":        2982,
  "excess":      5910,
  "dc_expected": 29548,
  "dc_delta":    27108,
  "ratio_ist":   0.92,
  "ratio_th":    0.50,
  "ladesperre":  0,
  "do4":         0,
  "peak_h":      13,
  "win_end_h":   16,
  "auto":        1,
  "ext_st":      0,
  "conflict":    0,
  "trace":       "POWER_MATCHING (Excess: 5910W, Budget: 7410W) | RAMP_LIMITED (5->1)"
}
```

| Feld | Einheit | Bedeutung |
|---|---|---|
| `state` | 0–7 | Entschiedener Lade-State (Bitmask CH1–CH3) |
| `changed` | 0/1 | Relais wurde in diesem Zyklus geschaltet |
| `pcc` | W | PCC-Leistung (+ = Einspeisung, − = Bezug) |
| `bat1` | W | Sofar-Batterie (+ = Entladung, − = Ladung) |
| `soc2` | % | EBox2-Ladestand |
| `soc1` | % | Sofar-Batterie-Ladestand |
| `ebox` | W | EBox2-Leistung (aus MQTT `ebox/pwr`) |
| `excess` | W | Berechneter Überschuss = pcc + ebox_eff + bat1 |
| `dc_expected` | W | Klarhimmel-Modell Ertrag jetzt |
| `dc_delta` | W | dc_expected − (pcc + ebox + bat1) |
| `ratio_ist` | 0–1+ | Ist-Wetter-Ratio (Anteil fehlender Klarhimmel-Leistung); `-1` = unbeurteilbar |
| `ratio_th` | 0–1 | Schwelle: ab `ratio_ist ≤ ratio_th` greift LADESPERRE (MQTT `sofar/ratio`, default 0.5) |
| `ladesperre` | 0/1 | LADESPERRE aktiv |
| `do4` | 0/1 | DO4-Puls ausgelöst |
| `peak_h` | h | Stunde des heutigen DC-Peaks (−1 = kein Peak >20kW) |
| `win_end_h` | h | Letzte Stunde mit dc_expected >20kW |
| `auto` | 0/1 | Auto-Modus aktiv |
| `ext_st` | 0–7 | Physischer Relay-State (aus CH1–CH3 Zustand) |
| `conflict` | 0/1 | ext_st weicht von final_state ab (nur bei auto=0 relevant) |
| `trace` | string | Entscheidungspfad (alle gefeuerten Regeln) |

**Mögliche trace-Werte:**

| trace | Bedeutung |
|---|---|
| `POWER_MATCHING (Excess: Xw, Budget: Yw)` | Normalbetrieb, bester State gewählt |
| `INSUFFICIENT_EXCESS (XW)` | Überschuss < 1200W → State 0 |
| `PCC_OVER_20KW (SOC=X% StateA→B)` | PCC >20kW, State erhöht |
| `RAMP_LIMITED (A->B)` | Hochschalten auf max. nächsten State begrenzt |
| `SWEET_SPOT_HOLD` | PCC nahe 0, kein Hochschalten |
| `TREND_BLOCK` | Überschuss-Trend negativ, kein Hochschalten |
| `BAT_GUARD_BLOCK` | Sofar-Batterie entlädt >220W, kein Hochschalten |
| `STABILIZING` | Zu wenige stabile Zyklen, kein Runterschalten |
| `HYSTERESIS` | Leistungsdiff <505W, kein Runterschalten |
| `EMERGENCY_FORCE` | Netzbezug >1020W, sofortiges Runterschalten |
| `GUARD:LADESPERRE_BIS_PCC_20KW` | LADESPERRE blockiert Laden (Gutwetter belegt, ratio_ist ≤ ratio_th) |
| `GUARD:BATTERY_FULL_STOP` | SOC2=100%, Laden gestoppt |
| `GUARD:CRITICAL_SOC_PROTECTION_ACTIVATE` | SOC2 <6%, Notladen State 1 |
| `MQTT_STALE_SAFE` | WR-Daten >3min alt, Zwangs-State 0 |

---

#### `sofar/waveshare/status` — Board-Telemetrie (alle 30s, retained)

```json
{
  "ip":           "192.168.178.187",
  "ip6":          "fe80::32ed:a0ff:fed8:fd44",
  "state":        1,
  "target_state": 1,
  "auto":         1,
  "ladesperre_en":1,
  "ratio_th":     0.50,
  "ratio_ist":    0.92,
  "peak_h":       13,
  "win_end_h":    16,
  "soyo_w":       0,
  "uptime":       7530,
  "mem_free":     247252,
  "fw":           "3.3.28",
  "fw_date":      "Jun 10 2026T08:37:33"
}
```

| Feld | Bedeutung |
|---|---|
| `state` | Physischer Relay-State (CH1–CH3 Bitmask) |
| `target_state` | Letzter von fox2db/state empfangener Wunsch-State |
| `auto` | Auto-Modus aktiv (1 = ESP32 ist Master) |
| `ladesperre_en` | LADESPERRE-Funktion aktiviert |
| `ratio_th` | Schwelle ab der LADESPERRE greift (MQTT `sofar/ratio`, default 0.5) |
| `ratio_ist` | Zuletzt berechnete Ist-Wetter-Ratio (`-1` = unbeurteilbar) |
| `peak_h` | Stunde des Tages-DC-Peaks |
| `win_end_h` | Ende des Peak-Fensters |
| `soyo_w` | Aktueller Soyo-Sollwert (W) |
| `uptime` | Sekunden seit Boot |
| `mem_free` | Freier Heap (Bytes); Produktion: ~247 KB |
| `fw` / `fw_date` | Firmware-Version und Build-Zeitstempel |

---

#### `fox2db/state` — Pi-Entscheidung (alle 60s, retained)

```json
{
  "ts":           "2026-06-09T12:19:02",
  "version":      "v2.9-Py",
  "soc_bat2":     68.4,
  "soc_bat1":     37.0,
  "pcc":          3410,
  "bat1":         2500,
  "ebox":         2982,
  "state":        1,
  "state_before": 0,
  "stable":       0,
  "excess":       5910,
  "drop_rate":    147.0,
  "trace":        "POWER_MATCHING (Excess: 5910W, Budget: 7410W) | RAMP_LIMITED (5->1)",
  "deep_discharge_active": false,
  "need_downward_regulation": false,
  "ladesperre":   false,
  "dc_peak_time": "13:00",
  "dc_window_end":"16:00"
}
```

| Feld | Bedeutung |
|---|---|
| `state` | Entschiedener State (Pi-Logik, ohne Relais-Wirkung bei auto=1) |
| `state_before` | State des letzten Zyklus |
| `stable` | Anzahl stabiler Zyklen seit letzter Änderung |
| `drop_rate` | W/s Änderung des Überschusses seit letztem Zyklus |
| `need_downward_regulation` | true wenn DO4-Puls nötig |

---

#### `ebox/pwr` — EBox2 BMS-Daten (alle 60s, retained)

```json
{
  "soc":       68.3,
  "power_w":  -80.6,
  "current_a": -1.52,
  "packs":     3,
  "ts":        "2026-06-09T12:19:31"
}
```

---

#### `soyo/calc` — Soyo-Sollwert-Kalkulation (alle 60s)

```json
{"W": 0, "soc2": 68.3, "stale": 0}
```

`stale=1` wenn WR-Daten >3min alt → Soyo auf 0W gesetzt.

---

#### `soyo/sent` — RS485-Frame (MQTT nur bei Wertänderung; RS485-TX läuft immer alle 3s)

```json
{"w": 468, "hex": "2456002101D4800F", "sends": 1840, "changes": 7}
```

`sends` = Gesamtzahl über RS485 gesendeter Frames seit Boot, `changes` = Anzahl Wertänderungen.

---

#### `rs485/rx` — Empfangene RS485-Frames

```json
{"len": 8, "hex": "245600210384800F"}
```

---

### Control-Topics (→ ESP32, schreibend)

#### Auto-Modus umschalten

```bash
# ESP32 übernimmt Steuerung (Produktion)
mosquitto_pub -h 192.168.178.218 -t sofar/auto -m '{"ENABLE":1}'

# Pi übernimmt Steuerung (fox2dbEasy.py-Modus)
mosquitto_pub -h 192.168.178.218 -t sofar/auto -m '{"ENABLE":0}'
```

#### LADESPERRE ein/aus

```bash
mosquitto_pub -h 192.168.178.218 -t sofar/ladesperre -m '{"ENABLE":1}'
mosquitto_pub -h 192.168.178.218 -t sofar/ladesperre -m '{"ENABLE":0}'
```

#### LADESPERRE-Schwelle (ratio_th) setzen

Schwelle ab der bei Schlechtwetter freigegeben wird (gültiger Bereich 0–1, default 0.5).
Nicht-retained → bei Reboot zurück auf 0.5.

```bash
# JSON-Form
mosquitto_pub -h 192.168.178.218 -t sofar/ratio -m '{"RATIO":0.6}'
# Nackte Zahl ebenfalls gültig
mosquitto_pub -h 192.168.178.218 -t sofar/ratio -m '0.6'
```

#### Soyo-Sollwert manuell setzen

```bash
# JSON-Form
mosquitto_pub -h 192.168.178.218 -t soyo/set -m '{"W":350}'
# Nackte Zahl ebenfalls gültig
mosquitto_pub -h 192.168.178.218 -t soyo/set -m '350'
```

#### Einzelrelais Hand-Test (nur für Diagnose, überschreibt auto!)

```bash
# CH1 ein, CH2 aus, CH3 ein  → State 5 (7100W) manuell
mosquitto_pub -h 192.168.178.218 -t waveshare/relay/1 -m '{"v":1}'
mosquitto_pub -h 192.168.178.218 -t waveshare/relay/2 -m '{"v":0}'
mosquitto_pub -h 192.168.178.218 -t waveshare/relay/3 -m '{"v":1}'

# Alle Relais aus
mosquitto_pub -h 192.168.178.218 -t waveshare/relay/all -m '{"v":0}'
```

#### RS485-Diagnose

```bash
# GPIO Edge-Scan (3s, Ergebnis auf rs485/scan_result)
mosquitto_pub -h 192.168.178.218 -t rs485/scan -m 'go'

# Relay-Physik-Test CH1→GPIO18 (Ergebnis auf rs485/relay_test_result)
mosquitto_pub -h 192.168.178.218 -t rs485/relay_test -m 'go'
```

#### Status sofort abrufen

```bash
# Letzten retained Status lesen (kein Warten nötig)
mosquitto_sub -h 192.168.178.218 -t sofar/waveshare/status -C 1
mosquitto_sub -h 192.168.178.218 -t fox2db/state -C 1
mosquitto_sub -h 192.168.178.218 -t ebox/pwr -C 1

# Live-Monitor aller fox2db-Topics
mosquitto_sub -h 192.168.178.218 -t 'sofar/#' -t 'fox2db/#' -t 'soyo/#' -v
```

---

## Betriebsmodi

| `auto` | Master | Beschreibung |
|:-:|---|---|
| 1 | ESP32 | fox2db_logic.h entscheidet autonom, setzt CH1–CH3 direkt (**Produktion**) |
| 0 | extern | ESP32 gibt CH1–CH3 frei für `waveshare/relay/1..3` (Hand/Diagnose) |

**Schatten-Vergleich:** `fox2dbEasy.py` (Pi) läuft als zustandsloser Zwilling der
ESP32-Logik und publiziert auf `fox2db/easy/state` — **steuert aber keine Relais**
(reiner Soll-Ist-Vergleich). Identische Inputs: PCC, `ebox/pwr` (signiert), `ratio_th=0.5`.
`waveshare_compare.py` meldet Abweichungen `fox2db/easy/state` ↔ `sofar/waveshare/status`.

Im `auto=1`-Modus bleibt zusätzlich `fox2db/state` (fox2db.py) aktiv:  
`target_state` im Status-JSON zeigt den letzten Pi-Wunsch, `conflict=1` wenn Abweichung.

---

## Safety

- Boot: alle Relais `ALWAYS_OFF`
- MQTT-Stale >3min: Zwangs-State 0, `auto=0` im JSON
- SOC2 <6%: CRITICAL_SOC_PROTECTION → State 1 (Notladen)
- SOC2 >100%: BATTERY_FULL_STOP → State 0
