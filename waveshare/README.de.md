# Sofar fox2db v2.9 — Waveshare ESP32-S3 6CH

Portiert die fox2db-Steuerlogik vom Pi auf das Waveshare 6-Kanal-Relayboard
(ESP32-S3) als ESPHome-Projekt. Basis: erprobte Config aus `gerontec/waveshare`.

## Dateien
| Datei | Zweck |
|-------|-------|
| `sofar_waveshare.yaml` | ESPHome-Config (MQTT, Relais, Zeit, Decision-Loop) |
| `fox2db_logic.h` | C++-Port: decide/guards/blocking + Solar-Forecast + Ladesperre |
| `open_wifi_scan.h` | WiFi-Helper (offene Netze, aus Basis-Repo) |

## Relais-Mapping
| Kanal | GPIO | Funktion |
|-------|------|----------|
| CH1 | 1  | EBox-State Bit0 |
| CH2 | 2  | EBox-State Bit1 |
| CH3 | 41 | EBox-State Bit2 (→ State 0..7 binär) |
| CH4 | 42 | **DO4** → WR2 abregeln (3s-Puls) |
| CH5 | 45 | frei |
| CH6 | 46 | frei |

## Flashen
```bash
cd ~/python/waveshare
# Erst-Flash über USB (Board an /dev/ttyACM0):
esphome run sofar_waveshare.yaml --device /dev/ttyACM0
# Danach OTA (kein Kabel):
esphome run sofar_waveshare.yaml --device sofar-waveshare.local
```
Vor dem Flash `wifi_password` in den `substitutions:` setzen.

## Erst-Test (sicher, in Reihenfolge)
1. **Boot:** alle Relais AUS (ALWAYS_OFF), Auto-Regelung AUS.
2. **Hand-Test Relais:**
   ```bash
   mosquitto_pub -h 192.168.178.218 -t waveshare/relay/1 -m '{"V":1}'   # CH1 an
   mosquitto_pub -h 192.168.178.218 -t waveshare/relay/all -m '{"V":0}' # alle aus
   ```
3. **EBox-Daten bereitstellen** (SOC2/Leistung) — Pi muss publizieren:
   ```bash
   mosquitto_pub -h 192.168.178.218 -t ebox/status -m '{"soc":50,"power":600}'
   ```
   Ohne `ebox/status` bleibt SOC2 = -1 → `EBOX_SOC_UNKNOWN_HOLD` (lädt nicht).
4. **Auto-Regelung scharf** (Core ohne Forecast/Ladesperre):
   ```bash
   mosquitto_pub -h 192.168.178.218 -t sofar/auto -m '{"ENABLE":1}'
   mosquitto_sub -h 192.168.178.218 -t sofar/state -v        # Entscheidungen beobachten
   ```
5. **Ladesperre/Forecast aktivieren** (erst nach Core-Verifikation):
   ```bash
   mosquitto_pub -h 192.168.178.218 -t sofar/ladesperre -m '{"ENABLE":1}'
   ```

## MQTT-Schnittstelle
| Richtung | Topic | Inhalt |
|----------|-------|--------|
| IN | `inverter/power_grid_exchange/json` | PCC, Bat1, SOC1 (vorhanden) |
| IN | `ebox/status` | `{"soc":..,"power":..}` SOC2 + EBox-Leistung — **TODO Pi** |
| OUT | `sofar/state` | Entscheidung (state, trace, excess, dc_delta, ladesperre, do4) |
| OUT | `sofar/waveshare/status` | Board-Telemetrie (heap, uptime) |
| CTRL | `sofar/auto`, `sofar/ladesperre` | `{"ENABLE":0/1}` |

## Pi-Seite (offen)
- **`ebox/status` publizieren:** Ein kleiner Pi-Dienst liest die EBox-BMS
  (wie `read_ebox()`) und published `{"soc":SOC2,"power":ebox_w}` jede Minute.
- **`sofar/state` → DB:** Subscriber schreibt in `pv_decision_log` (ersetzt das
  bisherige DB-Logging in fox2db.py). Damit bleiben `decision_report.py` und die
  Diagramme unverändert nutzbar.

## Unterschiede zum Pi (bewusst)
- Tages-Flags (do4_today/weather/reblock) im RAM → Reboot vor Mitternacht setzt
  die Ladesperre-Phase zurück. Für Produktiv ggf. `restore_value: true`.
- Solar via NOAA-Algorithmus statt `astral` → ~1 % Abweichung, unkritisch.
- Nur SOC2 steuert; SOC1 (Sofar) autark.

## Sicherheit
- `restore_mode: ALWAYS_OFF` → Boot/Reset = EBox aus, DO4 aus.
- MQTT-Stale > 3 min → Zwangs-State 0.
- Hardware-Watchdog: ESPHome-Framework reboot bei Hänger; WiFi `reboot_timeout 5min`.
