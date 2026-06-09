# Waveshare ESP32-S3-Relay-6CH — Produktionsdokumentation

**Firmware:** fox2db v3.3.21  
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
- `MIN_EXCESS = 1010 W` — Mindestüberschuss für Laden
- `MAX_GRID_DRAW = 1500 W` — max. erlaubter Netzbezug beim Schalten
- `HYSTERESIS = 505 W` — Mindest-Leistungsdiff für Runterschalten
- `STABILIZATION = 2` — Zyklen stabil vor Runterschalten
- `PCC_PEAK_TH = 20000 W` — Schwelle für DO4-Trigger / LADESPERRE-Freigabe
- `PCC_HARD_TH = 22000 W` — bedingungsloser DO4-Puls

### LADESPERRE

Hält den Ladestart zurück bis der Tages-PCC-Peak gesehen wurde:

```
ladesperre = has_peak && local_hour < win_end_h && !peak_today
```

Freigabe durch:
- `pcc > 20 kW` → peak_today=true, ab jetzt laden
- Schlechtwetter: `ratio = (dc_expected − (pcc_avg + ebox + bat1)) / dc_expected > 0.8`
- `local_hour >= win_end_h` (Peak-Fenster abgelaufen)

### DC-Klarhimmel-Modell (Meinel)

Berechnet stündlichen Ertrag für Ladesperre-Ratio und DO4-Fenster:
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

### RS485-Frame (alle 3s, nur bei Wertänderung publiziert)

```
[0x24, 0x56, 0x00, 0x21, PH, PL, 0x80, CRC]
CRC = (264 − PH − PL) & 0xFF
```

MQTT `soyo/sent`: `{"w":900,"hex":"245600210384800F","sends":120,"changes":3}`  
MQTT `soyo/calc`: `{"W":468,"soc2":63.1,"stale":0}`

---

## MQTT-Topics

### Eingänge (→ ESP32)

| Topic | Inhalt |
|---|---|
| `inverter/power_grid_exchange/json` | PCC, Bat1, SOC1 (Sofar-Wechselrichter) |
| `ebox/pwr` | `{"soc":63.1,"power_w":1200}` — EBox2 BMS |
| `pv_zaehl2` | `{"wirkleist":-900}` — Z2-Zähler PCC-Fallback |
| `fox2db/state` | Pi-Entscheidung (state, soc_bat2) → fox_relay_state |
| `sofar/auto` | `{"ENABLE":1}` — Auto-Modus ein/aus |
| `sofar/ladesperre` | `{"ENABLE":1}` — LADESPERRE ein/aus |
| `soyo/set` | `{"W":350}` — Soyo-Sollwert manuell |
| `waveshare/relay/1..6` | `{"v":1}` — Einzelrelais Hand-Test |

### Ausgänge (← ESP32)

| Topic | Inhalt |
|---|---|
| `sofar/state` | Entscheidung: state, pcc, bat1, soc2, excess, trace, ladesperre, do4, conflict |
| `sofar/waveshare/status` | Telemetrie 30s: state, target_state, auto, fw, fw_date, uptime, mem_free |
| `soyo/sent` | RS485-Frame bei Änderung: w, hex, sends, changes |
| `soyo/calc` | Sollwert-Kalkulation: W, soc2, stale |
| `rs485/rx` | Empfangene RS485-Frames |

---

## Betriebsmodi

| `auto` | Master | Beschreibung |
|:-:|---|---|
| 1 | ESP32 | fox2db_logic.h entscheidet autonom, setzt CH1–CH3 direkt |
| 0 | fox2dbEasy.py (Pi) | Pi sendet `waveshare/relay/1..3` + `sofar/auto:ENABLE=0` |

Im `auto=1`-Modus bleibt `fox2db/state` (Pi) als Schatten aktiv:  
`target_state` im Status-JSON zeigt den letzten Pi-Wunsch, `conflict=1` wenn Abweichung.

---

## Safety

- Boot: alle Relais `ALWAYS_OFF`
- MQTT-Stale >3min: Zwangs-State 0, `auto=0` im JSON
- SOC2 <6%: CRITICAL_SOC_PROTECTION → State 1 (Notladen)
- SOC2 >100%: BATTERY_FULL_STOP → State 0
