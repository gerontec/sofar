#!/usr/bin/env python3
"""
getdceast MMDDHH      →  expected PV DC watts WR2 (Stundenmitte :30)
getdceast MMDDHHMM    →  expected PV DC watts WR2 (exakte Minute)

WR2 (15 kWp, kein MQTT) — drei kalibrierte Arrays:
  PV2_Ost : tilt=41°, azimut=-74° von Süd (SE),      P_peak=19852 W
  PV2_West: tilt=60°, azimut=+90° von Süd (West),    P_peak= 2078 W
  PV1_West: tilt=32°, azimut=+94° von Süd (West),    P_peak= 2378 W

Kalibrierung:
  Datenquelle: REALTIME_REPORT_ROW_60AT203021WD014_67.csv
  Klartage: Mai 28, 29, 31 2026 (cv_pv2 < 0.37)
  Methode:
    PV2_West  → LS-Fit auf 18:30-20:00 (Ost-Anteil = 0)
    PV2_Ost   → LS-Fit auf 09-12h abzgl. West-Beitrag, mehrere Klartage
    PV1_West  → LS-Fit auf 14:30-20h
  KT Mai = 0.909 (gleicher Standort wie WR1/getdc)

Bekannte Ungenauigkeiten:
  - 08:00-09:00: Modell überschätzt (Morgen-Verschattung nicht modelliert)
  - 13:00-17:00: Modell unterschätzt ~1 kW (konservativ für Peak-Risk)
  - Andere Monate: KT aus WR1 übernommen (TODO eigene Kalibrierung)

Atmosphäre: Meinel-Klarhimmel  T = 0.7^(AM^0.678)
"""
import sys, math, datetime as dt, zoneinfo
from astral import LocationInfo
from astral.sun import elevation as sun_elevation, azimuth as sun_azimuth

LAT, LON = 47.6811, 11.5732
TZ       = "Europe/Berlin"

# Drei Arrays: [tilt_deg, az_from_S_deg, ppeak_W]
# az_from_S: 0=Süd, +90=West, -90=Ost
ARRAYS = [
    (41, -74, 19_852),   # PV2 – SE/Ost, dominante Strings (tilt=41°, az=−74°)
    (60,  90,  2_078),   # PV2 – West-Anteil             (tilt=60°, az=+90°)
    (32,  94,  2_378),   # PV1 – West                    (tilt=32°, az=+94°)
]

# Monatlicher Clearness-Index — übernommen von WR1 (gleicher Standort)
# Mai 2026 kalibriert, andere Monate TODO
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
    kt   = KT.get(t.month, 0.60)
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
    sys.exit("Usage: getdceast MMDDHH  oder  getdceast MMDDHHMM")
print(f"{dc_forecast(t):.0f}")
