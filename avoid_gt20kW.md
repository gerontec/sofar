# Summer Feed-In Problem: Avoiding >20 kW Grid Peak

## Key Figures

| Parameter | Value |
|---|---|
| Highest ever measured feed-in peak | **−35,269 W (−35.3 kW)** — 02 Jul 2024 |
| Typical summer peaks (top 10) | −33 to −35 kW, May–July |
| Battery EBox (BAT2) | **30 kWh** total |
| SOC minimum morning (27 May 2026) | **69 %** at 06:21 |
| Available charge capacity at 69 % | **9.3 kWh** |
| Summer assumption: SOC never below 50 % | max. **15 kWh** charge capacity |
| State 7 (maximum) | 11,400 W |
| Gap at 35 kW peak + max charging | ~24 kW uncompensated |

## PV Peak vs. Solar Noon — Offset Analysis (May 2026)

| Date | PV Peak | Power | Offset |
|---|---|---|---|
| 27 May 2026 | 14:25 | 15.3 kW | +74 min |
| 26 May 2026 | 13:56 | 11.9 kW | +46 min |
| 25 May 2026 | 13:49 | 12.2 kW | +39 min |
| 21 May 2026 | 13:39 | 17.6 kW | +29 min |
| 20 May 2026 | 13:42 | 19.3 kW | +32 min |
| **Average** | | | **+33 min** |

**Conclusion:** The installation has a **slight west orientation** (~+30 min on clear days).
The high daily scatter (−77 to +113 min) is due to cloud cover.
Analysis script: `pv_solar_plot.py [YYYY-MM-DD]`

## Sun Position Lenggries (47.68°N, 11.57°E)

- Script: `/home/pi/python/sunrise.py` (astral library)
- Output: `date, sunrise+60min, sunset−60min`
- Today (27 May 2026): **06:24 → 19:58**
- Solar noon: approx. **13:10 CEST**
- Summer peaks typically occur **11:00–15:00**

## Problem

In winter the control script (`fox2dbOO.py`) works perfectly:
- Battery arrives in the morning with low SOC
- Script charges during the day at maximum State 7 (11.4 kW)
- PV surplus is fully buffered

In summer this approach fails:
- Battery often starts the day at 69–93 % SOC
- Remaining charge capacity only 7–9 kWh
- Battery is already full by 09:30–10:30
- From then on the entire PV surplus is fed into the grid
- Midday peak: up to −35 kW

## Solution Idea: Stretch Charging Duration (DRM — Demand Response Management)

**Principle:** Throttle charging power in the morning so the battery only reaches
full charge around 12:00–13:00 and can absorb the midday peak completely.

### Summer Charging Schedule (Draft)

| Time Window | SOC Target | Max State | Reason |
|---|---|---|---|
| Sunrise +60min … 10:00 | ≤ 75 % | 2 (3,650 W) | slow charge, keep buffer open |
| 10:00 … 12:00 | ≤ 90 % | 4 (3,900 W) | moderate increase |
| 12:00 … 14:00 | 100 % | 7 (11,400 W) | full power for the peak |
| 14:00 … Sunset −60min | like winter | 7 | continue normally |

### DO4 Relay: Last Resort Only

Two triggers, both active:

| Trigger | Threshold | Note |
|---|---|---|
| PCC (FoxESS) | > 20,600 W | primary, measured directly at inverter |
| wirkleist (Z2) corrected | < −20,100 W | secondary, with heat pump offset (see below) |

**Important — Z2 does not measure the heat pump:**
Meter Z2 (`pv_zaehl2`) measures only part of the house network and does **not** see
the heat pump load (~4–10 kW). Without correction, Z2 would trigger at e.g. −16 kW
even though the heat pump consumes 4.6 kW of that, making the real grid export only
−11 kW → **false trigger**.

**Correction in script:**
```python
elif (vals.wirkleist + vals.wp_power) < self._cfg.wirkleist_r4_threshold:
```
- `vals.wp_power` comes from MQTT topic `em0/54/power` (Shelly EM0 measurement of heat pump)
- Effective value = wirkleist + heat pump power
- Example: wirkleist=−24 kW, WP=4.6 kW → corrected=−19.4 kW → no trigger

**DO4 is emergency protection, not a control strategy.**

## Implemented Solution: FeedInLimiter (28 May 2026)

**File:** `/home/pi/python/fox2dbOO.py` — replaces the faulty `FeedInCap` block.

### Architecture

| Component | Function |
|---|---|
| **fox2db** | controls **charging** of EBox (relay State 0–7 = charge power 0–11,400 W) |
| **SOYO** | controls **discharging** of EBox (separate system, independent) |

### FeedInLimiter Control Logic (as of 28 May 2026)

```
Active only: April–September (summer), not during deep-discharge protection

Measurement: PCC (ActivePower_PCC_Total × 1000 W) — direct grid feed-in reading
             Fallback to Z2 (pv_zaehl2.wirkleist) when PCC = NaN (mode switch)

feed_in < 19,000 W  →  FeedInLimiter OFF  — winter rule (POWER_MATCHING) takes over
feed_in ≥ 19,000 W  →  FeedInLimiter ON:
    absorption_needed = feed_in − 19,000 W
    target = find_min_covering_state(absorption_needed)
             → smallest state whose charge power ≥ absorption_needed
    Examples:  20 kW → absorb=1 kW  → State 1 (3.0 kW)  → PCC ~17 kW
               25 kW → absorb=6 kW  → State 3 (6.65 kW) → PCC ~18.4 kW
               30 kW → absorb=11 kW → State 7 (11.4 kW) → PCC ~18.6 kW

forecast = feed_in + drop_rate × 60 s  (capped at 35,000 W)
forecast ≥ 22,000 W  →  FeedInLimiter PRELOAD → pre-activate State 1

> Solar Noon + 2h  →  FeedInLimiter AFTER_PEAK → disabled, winter rule charges at max
```

**Threshold:** 19 kW (not 20 kW) — 1 kW control reserve before DO4 emergency protection (20.6 kW).

**Why PCC instead of Z2+WP:** Z2 does not capture the heat pump load. The earlier
correction `Z2 + WP` produced only 16.5 kW instead of the real 21.5 kW feed-in when
the heat pump (4.6 kW) was running — FeedInLimiter would not have triggered. PCC
measures the grid feed-in directly at the inverter and is always correct.

**PCC = NaN:** When FoxESS omits the `ActivePower_PCC_Total` field from the MQTT
packet (mode switch), PCC is set to NaN instead of 0. All `> 500` checks
automatically evaluate to False → Z2 fallback is used. The `decide()` function
(winter rule) uses the same fallback.

**Winter (Oct–Mar):** FeedInLimiter disabled — EBox charges at maximum.

**After Solar Noon + 2h:** FeedInLimiter disabled — if SOC is not yet 100%,
winter rule charges at maximum State 7 until evening.

### Log Examples (28 May 2026)

```
# FeedInLimiter activates at 19.5 kW:
FeedInLimiter ON [PCC] (19490W >= 19000W, absorb=490W) State1→1

# PCC below threshold — winter rule keeps State 1:
FeedInLimiter OFF [PCC] (17790W < 19000W, prognose=11890W)

# Mode switch: PCC=NaN, Z2 fallback used:
FeedInLimiter OFF [Z2+WP] (11735W < 19000W, prognose=-23805W)

# After 15:10 (Noon+2h):
FeedInLimiter AFTER_PEAK (>15:10 → winter rule)

# Bad weather — FeedInLimiter disabled:
FeedInLimiter BADWEATHER (dc_pv=2.10kW / theoretical=12.02kW = 0.17 < 0.25 elev=45.0°)
```

### Bad Weather Exception (Option B — PV Heuristic)

In bad weather the FeedInLimiter should be **disabled** so the EBox charges normally
(the small amount of PV energy must not be lost).

**Heuristic:** Compare current DC power with theoretical maximum:

```
theoretical_kw = system_peak_kw × sin(solar_elevation°)
ratio          = dc_pv / theoretical_kw
if ratio < feedin_bad_weather_ratio (0.25) → FeedInLimiter BADWEATHER → normal charging
```

**Prerequisite:** Solar elevation >= `feedin_min_elevation` (15°), otherwise no valid estimate.

### All Config Parameters

| Parameter | Default | Description |
|---|---|---|
| `feedin_season_start_month` | 4 | April: FeedInLimiter active |
| `feedin_season_end_month` | 9 | September: last active month |
| `feedin_limit_w` | 19,000 | Feed-in threshold above which charging starts (W) |
| `feedin_preload_w` | 22,000 | Forecast threshold for PRELOAD (W) |
| `feedin_max_prognose_w` | 35,000 | Forecast cap = all-time maximum (W) |
| `feedin_after_noon_hours` | 2.0 | AFTER_PEAK starts at Noon + X hours |
| `feedin_system_peak_kw` | 17.0 | Installed DC peak power of the system |
| `feedin_bad_weather_ratio` | 0.25 | dc_pv/theoretical below this → bad weather |
| `feedin_min_elevation` | 15.0 | Minimum solar elevation for bad weather heuristic |

**Known limitations:** Bad weather heuristic reacts to current cloud cover, no look-ahead.
For real forecasting → Option A+C (Open-Meteo via cron job) as future extension.

### Open Items

- [ ] Check hysteresis (e.g. stop at 17 kW, start at 19 kW) if oscillation occurs
- [ ] Backtest with pv_zaehl2 data 2024/2025 to verify 19 kW threshold is sufficient
- [ ] Tune feedin_bad_weather_ratio using summer 2026 data
- [ ] DO4 emergency: step up charge state first before firing relay pulse (get_next_state_up)
