# fox2dbOO — Solar-Ladesteuerung mit Wetterprognose

`fox2dbOO.py` (v1.58-Py) steuert eine SOYO-Einspeisung + EBox-Batteriespeicher
auf Basis von MQTT-Messdaten des Sofar-Wechselrichters.

## Architektur

```
MQTT (Sofar) ──┐
MQTT (Zähler) ─┤   fox2dbOO.py   ──► ebyte_ctrl.py  (Relais R1–R4)
EBox pwr ──────┤   FbController      ebox1arg.py     (EBox-Ladung)
               │   FeedInLimiter     MQTT-Publish    (fox2db/state)
Forecast API ──┘   WeatherForecast
```

## FeedInLimiter

Der Sommer-Regler hält die Netzeinspeisung unter `feedin_limit_w` (Standard 19 kW):

| Zustand | Bedingung | Aktion |
|---------|-----------|--------|
| **ON** | `feed_in ≥ limit` | Proportionalregelung: Stufe aus Überschuss |
| **PRELOAD** | `prognose ≥ preload_w` **und** Forecast OK | State 1 vorschalten |
| **PRELOAD_FC_OFF** | `prognose ≥ preload_w` **aber** Bewölkung ≥ Schwelle | State 0 — kein Vorschalten |
| **OFF** | sonst | State 0 |
| **AFTER_PEAK** | nach Solar Noon + X h | Winterregel / BRAKE |

## Wetterprognose (WeatherForecast)

### Datenquelle

OpenWeatherMap-Forecast für Lenggries (47.6833° N, 11.5667° O), stündlich
geschrieben von `heissa.de` in `wagodb.weather_data`.

**API-Endpunkt:**
```
GET https://web1.heissa.de/web1/forecast_api.php?hours=4
```

**Antwort (JSON):**
```json
{
  "lat": 47.6833,
  "lon": 11.5667,
  "rows": [
    {
      "timestamp": "2026-05-29 11:00:00",
      "weather": "clear sky",
      "cloudiness": 6,
      "pop": 0.0,
      "rain_3h": 0.0,
      "sunrise": "2026-05-29 05:22:14",
      "sunset":  "2026-05-29 21:00:20"
    }
  ]
}
```

### Logik

`WeatherForecast.solar_expected()` berechnet die mittlere Wolkenbedeckung
der nächsten `feedin_forecast_hours` Stunden:

```
avg_clouds = Ø cloudiness der nächsten 4 Stunden (%)
solar_ok   = avg_clouds < feedin_forecast_bad_clouds (Standard 70 %)
```

- `solar_ok = True` → PRELOAD darf schalten (Sonne erwartet)
- `solar_ok = False` → PRELOAD wird unterdrückt (`PRELOAD_FC_OFF`)
- Kein API-Ergebnis → `solar_ok = True` (fail-open)
- Cache-Dauer: 30 Minuten

**Beispiel-Trace im Log:**
```
FeedInLimiter PRELOAD [PCC] (18200W + trend=+42W/s×60s → prognose=20720W >= 22000W,
  forecast_clouds=8%<70%) State0→1
```
```
FeedInLimiter PRELOAD_FC_OFF [PCC] (forecast_clouds=82%>=70%) State1→0
```

### Konfigurationsparameter

| Parameter | Standard | Bedeutung |
|-----------|----------|-----------|
| `--feedin-forecast-url` | `https://web1.heissa.de/web1/forecast_api.php` | API-Endpunkt |
| `--feedin-forecast-hours` | `4` | Stunden voraus für Bewölkungs-Mittel |
| `--feedin-forecast-bad-clouds` | `70.0` | Schwelle % — darüber kein PRELOAD |

## Deployment

```
pi@192.168.178.119:/home/pi/python/fox2dbOO.py
```

Wird per `timeout 55 python3 fox2dbOO.py` jede Minute aus Cron gestartet.
Log: `/tmp/fox2db.log` (max. 122 kB, dann Truncate).

## Abhängigkeiten

```
pip install astral pymysql paho-mqtt
```
