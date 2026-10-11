#!/usr/bin/env python3
"""rep_heizung.py - monthly heat report: buffer heat split into DHW and heating circuits.

Source: wagodb on kellertreppe (192.168.178.218), table ww_share_daily,
written hourly by ww_share_daily.py on heissa (data since 2026-07-03).

Shares are energy weighted (sum dhw_kwh / sum th_kwh), not averages of daily
percentages. The current month and today are incomplete.

Usage: ./rep_heizung.py [--year YYYY] [--from YYYY-MM-DD] [--to YYYY-MM-DD]
"""
import argparse
from datetime import date

import pymysql

DB = dict(host='192.168.178.218', user='gh', password='a12345',
          database='wagodb', charset='utf8mb4', connect_timeout=8)

SQL = """SELECT DATE_FORMAT(day, '%%Y-%%m') AS month, COUNT(*) AS days,
                SUM(th_kwh), SUM(dhw_kwh), SUM(heat_kwh), SUM(COALESCE(solar_kwh, 0))
           FROM ww_share_daily
          WHERE day BETWEEN %s AND %s
          GROUP BY month ORDER BY month"""


def pct(part, total):
    return f'{100 * part / total:5.1f}' if total else '    -'


def main():
    ap = argparse.ArgumentParser(description='Monthly DHW / heating share of buffer heat')
    ap.add_argument('--year', type=int, help='calendar year (default: all data)')
    ap.add_argument('--from', dest='start', type=date.fromisoformat, help='first day (incl.)')
    ap.add_argument('--to', dest='end', type=date.fromisoformat, help='last day (incl.)')
    a = ap.parse_args()

    start = a.start or (date(a.year, 1, 1) if a.year else date(2000, 1, 1))
    end = a.end or (date(a.year, 12, 31) if a.year else date(2100, 1, 1))

    with pymysql.connect(**DB) as conn, conn.cursor() as cur:
        cur.execute(SQL, (start, end))
        rows = cur.fetchall()
    if not rows:
        print('no data in range')
        return

    head = f"{'month':8} {'days':>4} {'total kWh':>10} {'DHW kWh':>9} {'DHW %':>6} " \
           f"{'heat kWh':>9} {'heat %':>6} {'solar kWh':>9}"
    print(head)
    print('-' * len(head))
    tot = [0, 0.0, 0.0, 0.0, 0.0]
    for month, days, th, dhw, heat, solar in rows:
        th, dhw, heat, solar = (float(x or 0) for x in (th, dhw, heat, solar))
        print(f'{month:8} {days:4d} {th:10.1f} {dhw:9.1f} {pct(dhw, th):>6} '
              f'{heat:9.1f} {pct(heat, th):>6} {solar:9.1f}')
        for i, v in enumerate((days, th, dhw, heat, solar)):
            tot[i] += v
    days, th, dhw, heat, solar = tot
    print('-' * len(head))
    print(f"{'total':8} {days:4d} {th:10.1f} {dhw:9.1f} {pct(dhw, th):>6} "
          f'{heat:9.1f} {pct(heat, th):>6} {solar:9.1f}')


if __name__ == '__main__':
    main()
