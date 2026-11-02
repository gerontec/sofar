#!/usr/bin/env python3
"""Daily max / avg load on the EPS (off-grid / backup) output of the Sofar HYD 20KTL-3PH, Lenggries.

    eps    = inverter_data.ActivePower_Load_Total (0x0504, kW, 0.01 kW steps)
             at = sample of the daily EPS max, L1 / L2 / L3 % = share of ActivePower_Load_R / S / T (0x050C /
             0x0514 / 0x051C) at that sample (100 / 3 = balanced)
             L1 / L2 / L3 max W = daily max of each phase on its own (the EPS limit is per phase, not only the sum);
             the footer gives the max per phase over the whole period, when it happened and the reserve up to
             --phase-limit (default 20000 W / 3: nominal power of the HYD 20K split evenly, an assumption -
             check the EPS rating in the Sofar datasheet). L1 / L2 / L3 max A = daily max of Current_Load_R / S / T
             (0x050B / 0x0513 / 0x051B, 0.01 A), U min = lowest Voltage_Load_R / S / T (0x0510 / 0x0518 / 0x0520)
    island = samples with SysState (0x0404) = 3 (EPS state) x 5 min, grid 0 = samples with Frequency_Grid = 0
             (no grid at the Sofar grid port), fault = samples with any of Fault1-30 (0x0405-0x0416,
             0x0432-0x043D; only the columns present, Fault13-18 are masked out by the Sofar) not 0

Sampling: read.py via sofar.sh, one snapshot every 5 min since 2026-10-28 (not a mean), so a load shorter than
5 min may be missed; only loads lasting > 5 min show up for sure. avg kWh = avg W x 24 h (estimate from the
samples present, n = number of samples).
The report starts at the earliest on START (2026-11-02): EPS_Control (0x1029) = 1 since about 10:05 that day, the
load registers were 0 before. Current, voltage, SysState and Fault1-30 are written since 2026-11-02 11:00
(read.py EPS_REGISTERS), '-' before.

    ./rep_loadeps.py              # last 7 days
    ./rep_loadeps.py -n 30
    ./rep_loadeps.py --from 2026-11-02 --to 2026-11-09
    ./rep_loadeps.py --csv
    ./rep_loadeps.py --phase-limit 5000
"""
import argparse
import csv
import os
import sys
from collections import defaultdict
from datetime import date, timedelta

import pymysql

DB_HOST = os.environ.get("SOFAR_DB_HOST", "192.168.178.218")
# credentials: SOFAR_DB_USER / SOFAR_DB_PASSWORD, otherwise section [wagodb] of ~/.my.cnf (user, password)
DB = dict(database="wagodb", charset="utf8mb4", connect_timeout=5)
if os.environ.get("SOFAR_DB_PASSWORD"):
    DB.update(user=os.environ.get("SOFAR_DB_USER", "gh"), password=os.environ["SOFAR_DB_PASSWORD"])
else:
    DB.update(read_default_file=os.path.expanduser("~/.my.cnf"), read_default_group="wagodb")
START = date(2026, 11, 2)
SAMPLE_MIN = 5

SQL_FAULTS = """
SELECT column_name FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'inverter_data' AND column_name REGEXP '^Fault[0-9]+$'
"""
SQL = """
SELECT DATE_FORMAT(timestamp, '%%Y-%%m-%%d %%H:%%i'),
       ActivePower_Load_Total, ActivePower_Load_R, ActivePower_Load_S, ActivePower_Load_T,
       Current_Load_R, Current_Load_S, Current_Load_T,
       Voltage_Load_R, Voltage_Load_S, Voltage_Load_T,
       SysState, Frequency_Grid, {faults}
FROM inverter_data WHERE timestamp >= %s AND timestamp < %s AND ActivePower_Load_Total IS NOT NULL
ORDER BY timestamp
"""


def kw2w(v):
    return None if v is None else float(v) * 1000


def load(cur, first, end):
    days = defaultdict(lambda: {"eps": [], "peak": None, "pmax": [None] * 3, "imax": [None] * 3,
                                "umin": None, "island": 0, "nogrid": 0, "fault": 0})
    # Fault1-30 columns only exist where the Sofar address mask has the register (Fault13-18 do not)
    cur.execute(SQL_FAULTS)
    faults = ", ".join(sorted((r[0] for r in cur.fetchall()), key=lambda c: int(c[5:]))) or "NULL"
    cur.execute(SQL.replace("{faults}", faults), (first, end))
    for row in cur.fetchall():
        m, tot = row[0], kw2w(row[1])
        ph = [kw2w(x) for x in row[2:5]]
        cur_a = row[5:8]
        volt = [x for x in row[8:11] if x is not None]
        state, f_grid, faults = row[11], row[12], row[13:]
        day = days[m[:10]]
        day["eps"].append(tot)
        if day["peak"] is None or tot > day["peak"][0]:
            day["peak"] = (tot, m, *ph)
        for i in range(3):
            if ph[i] is not None and (day["pmax"][i] is None or ph[i] > day["pmax"][i][0]):
                day["pmax"][i] = (ph[i], m)
            if cur_a[i] is not None and (day["imax"][i] is None or float(cur_a[i]) > day["imax"][i]):
                day["imax"][i] = float(cur_a[i])
        if volt and (day["umin"] is None or float(min(volt)) < day["umin"]):
            day["umin"] = float(min(volt))
        day["island"] += state == 3
        day["nogrid"] += f_grid is not None and float(f_grid) == 0
        day["fault"] += any(x for x in faults if x is not None)
    return days


def stats(values):
    if not values:
        return None, None, 0
    return max(values), sum(values) / len(values), len(values)


def peak(p):
    """(time hh:mm, [L1, L2, L3 in % of the EPS load]) at the EPS max"""
    if p is None or p[0] <= 0 or None in p[2:]:
        return "-", [None] * 3
    return p[1][11:], [x * 100 / p[0] for x in p[2:]]


def pct(v):
    return "-" if v is None else f"{v:.0f}"


def w(v):
    return "-" if v is None else f"{float(v):.0f}"


def amp(v):
    return "-" if v is None else f"{v:.2f}"


def volt(v):
    return "-" if v is None else f"{v:.1f}"


def kwh(v):
    return "-" if v is None else f"{float(v) * 24 / 1000:.2f}"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("-n", type=int, default=7, help="last N days including today (default 7)")
    ap.add_argument("--from", dest="date_from", type=date.fromisoformat)
    ap.add_argument("--to", dest="date_to", type=date.fromisoformat)
    ap.add_argument("--csv", action="store_true", help="CSV output (;)")
    ap.add_argument("--phase-limit", type=float, default=20000 / 3,
                    help="continuous W per phase for the reserve (default 20000/3, assumption)")
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
        x = days[d]
        pm = [p[0] if p else None for p in x["pmax"]]
        rows.append((d, *stats(x["eps"]), *peak(x["peak"]), pm, x["imax"], x["umin"],
                     x["island"] * SAMPLE_MIN, x["nogrid"] * SAMPLE_MIN, x["fault"] * SAMPLE_MIN))

    if a.csv:
        out = csv.writer(sys.stdout, delimiter=";")
        out.writerow(["day", "eps_max_w", "eps_avg_w", "eps_kwh", "eps_n", "eps_max_at",
                      "max_l1_pct", "max_l2_pct", "max_l3_pct", "l1_max_w", "l2_max_w", "l3_max_w",
                      "l1_max_a", "l2_max_a", "l3_max_a", "u_min_v", "island_min", "no_grid_min", "fault_min"])
        for d, em, ea, en, at, sh, pm, im, um, isl, ng, fl in rows:
            out.writerow([d, w(em), w(ea), kwh(ea), en, at, *map(pct, sh), *map(w, pm), *map(amp, im),
                          volt(um), isl, ng, fl])
        return

    print(f"{'':10}  {' EPS output (Sofar 0x0504..) ':-^60}  {' current / voltage ':-^25}  {' minutes ':-^18}")
    print(f"{'day':10}  {'max W':>6} {'avg W':>6} {'kWh':>6} {'n':>4}  {'at':>5} {'L1 %':>4} {'L2 %':>4} {'L3 %':>4}  "
          f"{'L1 max':>6} {'L2 max':>6} {'L3 max':>6}  {'L1 A':>5} {'L2 A':>5} {'L3 A':>5} {'U min':>7}  "
          f"{'isl':>4} {'grid0':>5} {'flt':>5}")
    for d, em, ea, en, at, sh, pm, im, um, isl, ng, fl in rows:
        print(f"{d}  {w(em):>6} {w(ea):>6} {kwh(ea):>6} {en:>4}  {at:>5} {pct(sh[0]):>4} {pct(sh[1]):>4} {pct(sh[2]):>4}  "
              f"{w(pm[0]):>6} {w(pm[1]):>6} {w(pm[2]):>6}  {amp(im[0]):>5} {amp(im[1]):>5} {amp(im[2]):>5} {volt(um):>7}  "
              f"{isl:>4} {ng:>5} {fl:>5}")
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
