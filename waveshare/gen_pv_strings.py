#!/usr/bin/python3
"""Feldparameter der beiden FoxESS-Strings einzeln fitten, mit Horizont-Term.

Bisher war ARRAYS_EAST ein Sammelposten aus drei Feldern, gefittet gegen die
Summe pvPower. Die Strings sind aber physisch verschieden:

  PV1  ~200 V,  max 6.4 A,  Peak 14-15 Uhr  ->  kleine Westflaeche
  PV2  ~500 V,  max 21.8 A, Peak 11-12 Uhr  ->  grosse Ostflaeche, morgens
                                                 hinter Baeumen

Damit laesst sich jeder String gegen seine eigene Messreihe fitten, und die
Ostverschattung wird als Horizont modelliert statt sie im Betrag zu verstecken:
liegt die Sonne unter der Horizontlinie, bleibt nur ein Diffusanteil.

Gefittet wird je String (tilt, azS, power) und global (horizont_ost,
horizont_sued, diffusanteil). Rein lesend, gibt nur Konstanten aus.
"""

import argparse
import math
import sys
import time

sys.path.insert(0, '/home/pi/python')
import fox2dbEasy as fx                    # noqa: E402
from db_config import get_db_connection    # noqa: E402

MIN_W = 200.0
AZ_SPLIT = 120.0        # Trennung Ost-/Suedsektor des Horizonts


def fetch(col, days, table="inverter_data2", extra=""):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""SELECT timestamp, %s*1000 FROM %s
                    WHERE %s IS NOT NULL %s AND DATE(timestamp) IN (%s)
                    ORDER BY timestamp""" % (col, table, col, extra,
                                             ",".join("'%s'" % d for d in days)))
    rows = cur.fetchall()
    conn.close()
    out = []
    for ts, p in rows:
        t_utc = time.mktime(ts.timetuple())
        elev, azN = fx.sun_pos(t_utc)
        if elev <= 0:
            continue
        horiz = fx.calc_arrays([(0.0, 0.0, 1.0)], t_utc, ts.month)
        atm = horiz / math.sin(math.radians(elev))       # = T * kt, ohne Nachbau
        out.append((ts, float(p), elev, azN, atm))
    return out


def model(pt, tilt, azS, power, hor_ost, hor_sued, diffus):
    _, _, elev, azN, atm = pt
    aoi = max(0.0, fx.cos_aoi(elev, azN, tilt, azS))
    h = hor_ost if azN < AZ_SPLIT else hor_sued
    f = 1.0 if elev >= h else diffus                     # unter dem Horizont nur Diffus
    return power * atm * aoi * f


def rmse(pts, *par):
    err = n = 0
    for pt in pts:
        m = model(pt, *par)
        if m < MIN_W and pt[1] < MIN_W:
            continue
        err += (pt[1] - m) ** 2
        n += 1
    return (math.sqrt(err / n) if n else float('inf')), n


def best_power(pts, tilt, azS, hor_ost, hor_sued, diffus):
    num = den = 0.0
    for pt in pts:
        b = model(pt, tilt, azS, 1.0, hor_ost, hor_sued, diffus)
        if b <= 0:
            continue
        num += pt[1] * b
        den += b * b
    return num / den if den else 0.0


def fit(pts, tilts, azs, hors_o, hors_s, diffs):
    best = None
    for tilt in tilts:
        for azS in azs:
            for ho in hors_o:
                for hs in hors_s:
                    for df in diffs:
                        p = best_power(pts, tilt, azS, ho, hs, df)
                        if p <= 0:
                            continue
                        r, n = rmse(pts, tilt, azS, p, ho, hs, df)
                        if best is None or r < best[0]:
                            best = (r, tilt, azS, p, ho, hs, df, n)
    return best


def main():
    ap = argparse.ArgumentParser(description="FoxESS-Strings einzeln fitten")
    ap.add_argument("--days", nargs="*", help="Tage YYYY-MM-DD")
    args = ap.parse_args()
    days = args.days or ['2025-09-03', '2025-10-03', '2025-11-01', '2025-12-02',
                         '2026-01-31', '2026-02-26', '2026-03-20', '2026-04-23',
                         '2026-05-28', '2026-06-27', '2026-07-02', '2026-08-08',
                         '2026-08-11', '2026-08-12']
    print("Datenbasis: %d klare Tage %s .. %s" % (len(days), days[0], days[-1]))
    print()

    QUELLEN = [
        ("pv2Power",   "FoxESS PV2 (Ost, gross)",  "inverter_data2", "", 0),
        ("pv1Power",   "FoxESS PV1 (West, klein)", "inverter_data2", "", 1),
        ("Power_PV1",  "Sofar PV1 (West)",  "inverter_data", "AND device_id='1'", 0),
        ("Power_PV2",  "Sofar PV2 (Sued)",  "inverter_data", "AND device_id='1'", 1),
    ]
    for col, label, table, extra, ref_idx in QUELLEN:
        pts = fetch(col, days, table, extra)
        # Ausgangslage: bisheriges Sammelmodell, anteilig am gemessenen Split
        ref = fx.ARRAYS_EAST[ref_idx] if table == "inverter_data2" else fx.ARRAYS[ref_idx]
        r_old, n = rmse(pts, *(tuple(ref) + (0.0, 0.0, 1.0)))
        # 1. ohne Horizont (nur Geometrie + Leistung)
        b0 = fit(pts, range(20, 61, 5), range(-85, 46, 5), [0.0], [0.0], [1.0])
        b0 = fit(pts, range(max(5, b0[1] - 4), b0[1] + 5), range(b0[2] - 4, b0[2] + 5),
                 [0.0], [0.0], [1.0])
        # 2. mit Horizont
        b1 = fit(pts, [b0[1]], [b0[2]], range(0, 33, 4), range(0, 33, 4),
                 [0.05, 0.10, 0.15, 0.25])
        b1 = fit(pts, range(max(5, b0[1] - 4), b0[1] + 5), range(b0[2] - 4, b0[2] + 5),
                 range(max(0, b1[4] - 3), b1[4] + 4), range(max(0, b1[5] - 3), b1[5] + 4),
                 [b1[6]])
        print("%s — %d Messpunkte" % (label, b1[7]))
        print("   bisheriges Sammelmodell   RMSE %6.0f W" % r_old)
        print("   Fit ohne Horizont         RMSE %6.0f W   (%2d°, %+3d°, %5.0f W)"
              % (b0[0], b0[1], b0[2], b0[3]))
        print("   Fit mit Horizont          RMSE %6.0f W   (%2d°, %+3d°, %5.0f W)"
              % (b1[0], b1[1], b1[2], b1[3]))
        print("      Horizont Ost %d°, Sued %d°, Diffusanteil %.2f  -> Verbesserung %.0f %%"
              % (b1[4], b1[5], b1[6], 100 * (1 - b1[0] / r_old)))
        print()


if __name__ == "__main__":
    main()
