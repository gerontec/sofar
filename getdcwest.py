#!/usr/bin/env python3
"""
getdcwest MMDDHH      →  expected PV DC watts WR1 (Stundenmitte :30)
getdcwest MMDDHHMM    →  expected PV DC watts WR1 (exakte Minute)

WR1 (Sofar-Inverter, Power_PV1+PV2 via MQTT) — zwei kalibrierte Arrays:
  PV1: tilt=25°, azimut=+80° von Süd (fast West),  P_peak=16438 W
  PV2: tilt=60°, azimut= -5° von Süd (fast Süd),   P_peak=6573 W

Kalibrierung:
  Datenquelle: inverter_data.Power_PV1+Power_PV2, 2025-12 bis 2026-05
  Methode: Ratio-Verteilung (1348 Stunden-Mittelwerte) — Klarhimmel-Peak bei
           ratio≈1.0 (10.5% aller Messstunden). KT-Werte validiert, nicht abgesenkt.
  Cloud-Edge-Effekte (ratio>1.4, 2.5% der Fälle) nicht modellierbar.
  Nur WR1 (Sofar). WR2 (15kWp Ost) → getdceast.py
"""
import sys, math, datetime as dt, zoneinfo
from astral import LocationInfo
from astral.sun import elevation as sun_elevation, azimuth as sun_azimuth

LAT, LON = 47.6811, 11.5732
TZ       = "Europe/Berlin"

# WR1 Arrays: [tilt_deg, az_from_S_deg, ppeak_W]
# az_from_S: 0=Süd, +90=West, -90=Ost
ARRAYS = [
    (25,  80, 27_854),   # PV1 – 25° Tilt, fast West
    (60,  -5, 11_138),   # PV2 – 60° Tilt, fast Süd
]

# Monatlicher Clearness-Index — validiert aus inverter_data Ratio-Verteilung (Klarhimmel-Peak ≈1.0)
KT = {1: 0.331, 2: 0.402, 3: 0.563, 4: 0.838,
      5: 0.909, 6: 0.880, 7: 0.840, 8: 0.820,
      9: 0.760, 10: 0.600, 11: 0.350, 12: 0.134}

_LOC = LocationInfo("Lenggries", "Germany", TZ, LAT, LON)
_TZ  = zoneinfo.ZoneInfo(TZ)


def _cos_aoi(elev_deg, az_sun_N, tilt_deg, az_panel_S):
    e  = math.radians(elev_deg)
    b  = math.radians(tilt_deg)
    da = math.radians((az_sun_N - 180.0) - az_panel_S)
    return math.sin(e) * math.cos(b) + math.cos(e) * math.sin(b) * math.cos(da)


def dc_forecast(t: dt.datetime) -> float:
    t_tz = t.replace(tzinfo=_TZ) if t.tzinfo is None else t
    elev = sun_elevation(_LOC.observer, t_tz)
    if elev <= 0:
        return 0.0
    az_N = sun_azimuth(_LOC.observer, t_tz)
    am   = min(1.0 / math.sin(math.radians(elev)), 37.0)
    T    = 0.7 ** (am ** 0.678)
    kt   = KT.get(t.month, 0.50)
    total = 0.0
    for tilt, az_s, ppeak in ARRAYS:
        coi = max(0.0, _cos_aoi(elev, az_N, tilt, az_s))
        total += ppeak * T * coi
    return total * kt


arg = sys.argv[1] if len(sys.argv) > 1 else ""
y = dt.date.today().year
if len(arg) == 6 and arg.isdigit():
    mo, d, h = int(arg[:2]), int(arg[2:4]), int(arg[4:6])
    t = dt.datetime(y, mo, d, h, 30)
elif len(arg) == 8 and arg.isdigit():
    mo, d, h, mi = int(arg[:2]), int(arg[2:4]), int(arg[4:6]), int(arg[6:8])
    t = dt.datetime(y, mo, d, h, mi)
else:
    sys.exit("Usage: getdcwest MMDDHH  oder  getdcwest MMDDHHMM")
print(f"{dc_forecast(t):.0f}")
