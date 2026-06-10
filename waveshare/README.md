# fox2db — autonomous PV‑surplus controller (Waveshare ESP32‑S3 6CH, ESPHome)

ESPHome firmware that decides **on‑device** how to turn PV export into stored energy
and how to trickle‑discharge a battery at night. All control runs on a Waveshare
**ESP32‑S3 6CH relay** board; Home Assistant only monitors and tunes over MQTT.

> 🇩🇪 German production notes: [`README.de.md`](README.de.md) · [`waveshare_6ch_esp32s3.md`](waveshare_6ch_esp32s3.md)

## What it does
- Charges an **"EBox" battery (~30 kWh)** from PV surplus at **8 discrete power
  levels (0 … 11.4 kW)**, selected as a **3‑bit relay code** (CH1–CH3).
- Discharges that battery back into the house at night via a **Soyo grid‑tie
  inverter (≤ 900 W)** over **RS485**.
- Curtails a second inverter on hard export (> 22 kW) with a **3 s DO4 pulse** (CH4).
- The Sofar hybrid inverter and its own ~5 kWh battery stay **autonomous** — the
  controller only *reads* their power, it never commands them.

Plant context: 35 kWp PV, 3×50 A grid connection.

## Beyond stock ESPHome
This is more than sensors/switches/automations in YAML:

- **Custom C++ control logic** (`fox2db_logic.h`, pulled in via `includes:`):
  a quantized **state‑machine controller** — *not a PID* — with dead‑band,
  hysteresis, ramp‑limiting and protection guards.
- **Hand‑rolled RS485 protocol** for the Soyo inverter (8‑byte frame + checksum,
  3 s keep‑alive) implemented in a 50 ms `interval:` lambda.
- **On‑device clear‑sky solar forecast** (NOAA sun position, computed in firmware)
  driving a predictive **"charge‑block"** that keeps battery headroom for the
  midday peak.
- **Formally modeled & validated**: a self‑testing Python reference
  (`fox2db_model.py`, `soyo_model.py`) reproduces the ESP's real decisions
  **100 % one‑step‑ahead** against logged data; auto‑generated flowchart/model PDFs.

## Relay / pin map
| Channel | GPIO | Function |
|---|---|---|
| CH1 | 1  | EBox‑state bit 0 |
| CH2 | 2  | EBox‑state bit 1 |
| CH3 | 41 | EBox‑state bit 2 → state 0..7 binary |
| CH4 | 42 | **DO4** → curtail 2nd inverter (3 s pulse) |
| CH5 / CH6 | 45 / 46 | free |
| RS485 | TX 17 / RX 18 / DE‑RE 3 | Soyo frame TX every 3 s, RX 25 ms gap |

## State table
| State | Power | CH1 | CH2 | CH3 |
|:-:|---:|:-:|:-:|:-:|
| 0 | 0 W | 0 | 0 | 0 |
| 1 | 3000 W | 1 | 0 | 0 |
| 2 | 3650 W | 0 | 1 | 0 |
| 3 | 6650 W | 1 | 1 | 0 |
| 4 | 3900 W | 0 | 0 | 1 |
| 5 | 7100 W | 1 | 0 | 1 |
| 6 | 7800 W | 0 | 1 | 1 |
| 7 | 11400 W | 1 | 1 | 1 |

## Control concept (fox2db v3.3.30)
Each **60 s** cycle (`step()` in `fox2db_logic.h`):

1. `excess = pcc + ebox_eff + bat1`
2. `budget = excess + MAX_GRID_DRAW (900 W)` → pick the highest state whose power ≤ budget (`INSUFFICIENT_EXCESS` if that is 0)
3. **ramp‑limit** (max one power step up per cycle)
4. **guards** (hard): charge‑block / battery‑full / critical‑SoC / deep‑discharge
5. **blocking**: `EMERGENCY_FORCE` (pcc < −1020 W, immediate shed) · `SWEET_SPOT_HOLD` (|pcc| < 160 W) · `TREND_BLOCK` · `BAT_GUARD_BLOCK` (bat1 < −110 W) · `STABILIZING` (2 cycles) · `HYSTERESIS` (505 W)

Charger (EBox) and discharger (Soyo) are mutually exclusive and tile the
grid‑exchange axis around 0 W → the grid stays near "sweet spot" even when a large
load (e.g. a heat pump) switches on. The Soyo side is a **gated P‑controller** on
grid import with a night feed‑forward baseline.

## Scheduling
A single cooperative ESPHome `App.loop()` (FreeRTOS `loopTask`, Arduino framework)
drives all `interval:` timers (`millis()`‑based, not preemptive): **60 s** decision
cycle, 50 ms RS485, 30 s telemetry. SNTP only supplies wall‑clock time for the
forecast. Details: [`waveshare_6ch_esp32s3.md`](waveshare_6ch_esp32s3.md#firmware-architektur--scheduling).

## MQTT interface (Home Assistant)
| Dir | Topic | Payload |
|---|---|---|
| IN | `inverter/power_grid_exchange/json` | PCC, Bat1, SOC1 |
| IN | `ebox/pwr` | `{"soc":…,"power_w":…}` SOC2 + EBox power |
| OUT | `sofar/state` | decision (state, trace, excess, ratio, ladesperre, do4, …) |
| OUT | `sofar/waveshare/status` · `soyo/calc` | telemetry / Soyo setpoint |
| CTRL | `sofar/auto` · `sofar/ladesperre` · `sofar/ratio` · `soyo/set` | `{"ENABLE":0/1}` etc. |

## Build & flash
```bash
# first flash over USB:
esphome run sofar_waveshare.yaml --device /dev/ttyACM0
# then OTA:
esphome compile sofar_waveshare.yaml          # always compile before OTA
esphome upload  sofar_waveshare.yaml --device <ip>
```
Set `wifi_ssid` / `wifi_password` in the `substitutions:` block first.

## Files
| File | Purpose |
|---|---|
| `sofar_waveshare.yaml` | ESPHome config (MQTT, relays, intervals, RS485) |
| `fox2db_logic.h` | C++ controller: decide / guards / blocking + solar forecast + charge‑block |
| `fox2db_model.py` · `soyo_model.py` | math reference models + self‑tests |
| `validate_model.py` | one‑step‑ahead validation against logged decisions |
| `make_fox2db_pdf.py` · `make_soyo_pdf.py` | generate the model PDFs |
| `fox2db_model.pdf` · `soyo_model.pdf` | math model docs (formulas, plots, validation) |

## Safety
`restore_mode: ALWAYS_OFF` (boot/reset = everything off) · MQTT stale > 3 min →
forced state 0 · `auto` defaults to **on** (ESP self‑regulates; an external
controller takes over only on explicit `sofar/auto {"ENABLE":0}`).
