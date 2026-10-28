#!/usr/bin/env python3
"""Daily max / avg load of the house and of the whole site behind Z1.

    house  = ac_out_l1_w + ac_out_l2_w + ac_out_l3_w           (AC-out1 of the MultiPlus = house, venus.heissa.de)
             at = minute of the daily house max, L1 / L2 / L3 % = share of each phase in the house load at that
             minute (100 / 3 = balanced)
             L1 / L2 / L3 max W = daily max of each phase on its own (planning aid: the limit is per MultiPlus = per
             phase, not the 12 kW sum); the footer gives the max per phase over the whole period, when it happened
             and the reserve up to --phase-limit (default 4000 W continuous per MultiPlus-II 48/5000 at 25 C, check
             the datasheet). Only loads lasting > 60 s matter (start-up peaks do not): victron2db.py stores a
             snapshot once a minute (not a mean), so every load lasting > 60 s is in at least one sample; a shorter
             peak may be caught by chance, which only makes the max more conservative. ovl = minutes with the
             VE.Bus overload LED on or blinking
    total  = Sofar load - Multis AC-in + Multis AC-out         (everything behind Z1 incl. heat pump, per minute)
             inverter_data.ActivePower_Load_Sys (kW, Sofar CT at Z1; contains the Multis AC-in, clipped at 0 by the
             Sofar, so Multi export reads a few 10 W too high), pv_victron.ac_in_l1..3_w (+ = from the grid) and
             ac_out_l1..3_w; Multi charging / losses do not count
    z1     = pv_Z1 counter difference import - export of the day (meter resolution 1 kWh, cross-check only)

Not used: inverter_data.ActivePower_Load_Total (always 0), ActivePower_Output_Total - PCC (below the export on
sunny days, negative load), pv_Z1.p_bezug_w (derived from the 1 kWh steps), pv_victron.consumption_l*_w (Venus
does not see the Sofar PV). Load_Sys matches the Sofar counter Load_Consumption_Today.
Minutes are joined on the minute of the timestamp; a minute counts only with a Sofar and a Victron row.
The report starts at the earliest on START (2026-10-27): AC-out1 feeds the house only since then.
Before that the Multis fed back over AC-in, which the Sofar load clips at 0 (total near 0 on 12.-20.10.).
avg kWh = avg W x 24 h (estimate from the minutes present, n = number of minutes).

    ./rep_load.py              # last 7 days
    ./rep_load.py -n 30
    ./rep_load.py --from 2026-10-01 --to 2026-10-27
    ./rep_load.py --csv
    ./rep_load.py --phase-limit 3500     # reserve against a lower continuous limit (warm room)
"""
import argparse
import csv
import os
import sys
from collections import defaultdict
from datetime import date, timedelta

import pymysql

DB_HOST = os.environ.get("VICTRON_DB_HOST", "192.168.178.218")
DB = dict(user="gh", password="a12345", database="wagodb",
          charset="utf8mb4", connect_timeout=5)
START = date(2026, 10, 27)

SQL_VICTRON = """
SELECT DATE_FORMAT(ts, '%%Y-%%m-%%d %%H:%%i'),
       ac_out_l1_w, ac_out_l2_w, ac_out_l3_w,
       ac_in_l1_w + ac_in_l2_w + ac_in_l3_w, led_overload
FROM pv_victron WHERE ts >= %s AND ts < %s
"""
SQL_SOFAR = """
SELECT DATE_FORMAT(timestamp, '%%Y-%%m-%%d %%H:%%i'), ActivePower_Load_Sys
FROM inverter_data WHERE timestamp >= %s AND timestamp < %s AND ActivePower_Load_Sys IS NOT NULL
"""
SQL_Z1 = """
SELECT DATE(ts), MAX(bezug_kwh) - MIN(bezug_kwh), MAX(einspeisung_kwh) - MIN(einspeisung_kwh)
FROM pv_Z1 WHERE ts >= %s AND ts < %s GROUP BY DATE(ts)
"""


def load(cur, first, end):
    """{day: {'house': [W, ...], 'peak': (W, minute, L1, L2, L3) at the house max, 'total': [W, ...], 'z1': kWh}}"""
    days = defaultdict(lambda: {"house": [], "peak": None, "pmax": [None] * 3, "overload": 0,
                                "total": [], "z1": None})
    cur.execute(SQL_VICTRON, (first, end))
    victron = {}
    for m, l1, l2, l3, inp, ovl in cur.fetchall():
        out = None if None in (l1, l2, l3) else l1 + l2 + l3
        victron[m] = (out, inp)
        day = days[m[:10]]
        day["overload"] += bool(ovl)
        for i, x in enumerate((l1, l2, l3)):
            if x is not None and (day["pmax"][i] is None or x > day["pmax"][i][0]):
                day["pmax"][i] = (x, m)
        if out is not None:
            day["house"].append(out)
            if day["peak"] is None or out > day["peak"][0]:
                day["peak"] = (out, m, l1, l2, l3)
    cur.execute(SQL_SOFAR, (first, end))
    for m, load_kw in cur.fetchall():
        v = victron.get(m)
        if v is None or None in v:
            continue
        out, inp = v
        days[m[:10]]["total"].append(float(load_kw) * 1000 - inp + out)
    cur.execute(SQL_Z1, (first, end))
    for d, imp, exp in cur.fetchall():
        days[f"{d:%Y-%m-%d}"]["z1"] = float(imp - exp)
    return days


def stats(values):
    if not values:
        return None, None, 0
    return max(values), sum(values) / len(values), len(values)


def peak(p):
    """(time hh:mm, [L1, L2, L3 in % of the house load]) at the house max"""
    if p is None or p[0] <= 0:
        return "-", [None] * 3
    return p[1][11:], [x * 100 / p[0] for x in p[2:]]


def pct(v):
    return "-" if v is None else f"{v:.0f}"


def w(v):
    return "-" if v is None else f"{float(v):.0f}"


def kwh(v):
    return "-" if v is None else f"{float(v) * 24 / 1000:.1f}"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("-n", type=int, default=7, help="last N days including today (default 7)")
    ap.add_argument("--from", dest="date_from", type=date.fromisoformat)
    ap.add_argument("--to", dest="date_to", type=date.fromisoformat)
    ap.add_argument("--csv", action="store_true", help="CSV output (;)")
    ap.add_argument("--phase-limit", type=float, default=4000, help="continuous W per phase for the reserve")
    a = ap.parse_args()

    if a.date_from or a.date_to:
        last = a.date_to or date.today()
        first = a.date_from or last
    else:
        last = date.today()
        first = last - timedelta(days=a.n - 1)
    first = max(first, START)
    if first > last:
        sys.exit("--from is after --to")

    conn = pymysql.connect(host=DB_HOST, **DB)
    try:
        with conn.cursor() as cur:
            days = load(cur, first, last + timedelta(days=1))
    finally:
        conn.close()

    rows = []
    for d in sorted(days):
        pm = [x[0] if x else None for x in days[d]["pmax"]]
        rows.append((d, *stats(days[d]["house"]), *peak(days[d]["peak"]), pm, days[d]["overload"],
                     *stats(days[d]["total"]), days[d]["z1"]))

    if a.csv:
        out = csv.writer(sys.stdout, delimiter=";")
        out.writerow(["day", "house_max_w", "house_avg_w", "house_kwh", "house_n",
                      "house_max_at", "max_l1_pct", "max_l2_pct", "max_l3_pct",
                      "l1_max_w", "l2_max_w", "l3_max_w", "overload_min", "total_max_w", "total_avg_w", "total_kwh", "total_n", "z1_net_kwh"])
        for d, hm, ha, hn, at, sh, pm, ovl, tm, ta, tn, z1 in rows:
            out.writerow([d, w(hm), w(ha), kwh(ha), hn, at, *map(pct, sh), *map(w, pm), ovl, w(tm), w(ta), kwh(ta), tn, w(z1)])
        return

    print(f"{'':10}  {' house (AC-out1) ':-^79}  {' total (behind Z1) ':-^36}")
    print(f"{'day':10}  {'max W':>7} {'avg W':>6} {'kWh':>6} {'n':>6}  {'at':>5} {'L1 %':>5} {'L2 %':>5} {'L3 %':>5}  {'L1 max':>6} {'L2 max':>6} {'L3 max':>6} {'ovl':>3}  "
          f"{'max W':>7} {'avg W':>6} {'kWh':>6} {'n':>6}  {'Z1 kWh':>6}")
    for d, hm, ha, hn, at, sh, pm, ovl, tm, ta, tn, z1 in rows:
        print(f"{d}  {w(hm):>7} {w(ha):>6} {kwh(ha):>6} {hn:>6}  {at:>5} {pct(sh[0]):>5} {pct(sh[1]):>5} {pct(sh[2]):>5}  "
              f"{w(pm[0]):>6} {w(pm[1]):>6} {w(pm[2]):>6} {ovl:>3}  "
              f"{w(tm):>7} {w(ta):>6} {kwh(ta):>6} {tn:>6}  {w(z1):>6}")
    if not rows:
        print("no rows")
        return

    print(f"\nper phase over {rows[0][0]} .. {rows[-1][0]}, reserve to {a.phase_limit:.0f} W continuous:")
    for i in range(3):
        best = max((days[d]["pmax"][i] for d in days if days[d]["pmax"][i]), default=None)
        if best is None:
            print(f"   L{i + 1}  -")
            continue
        print(f"   L{i + 1}  max {best[0]:>5.0f} W  at {best[1]}  ({best[0] * 100 / a.phase_limit:3.0f} %)"
              f"  reserve {a.phase_limit - best[0]:>5.0f} W")


if __name__ == "__main__":
    main()
