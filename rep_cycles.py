#!/usr/bin/python3
# Compressor cycles of the R290 heat pump: one line per run (start, end, run and pause
# minutes, defrost minutes, mean Hz, kWh_el, tank start -> end, outlet max, T4) and a
# per-day summary (starts, run hours, duty, median run/pause, short runs < 20 min).
#
# Cycle = consecutive minutes with Hz > 0; missing rows up to 5 min are bridged, one row with
# Hz 0 ends the cycle (same rule as rep_freq.py / rep_superheat.py), but WITHOUT the >= 10 min
# filter: short runs are the point.
# Defrost minutes stay inside the cycle. A cycle counts for the day it starts on.
# Energy: compressor_operating_power * 100 W per minute.
#
# Run on pi@192.168.178.218 (kellertreppe): rep_cycles.py [--days 2] [--short 20]
# Needs pymysql.

import argparse
import statistics as st
from datetime import datetime, timedelta

import pymysql

DB = dict(host='192.168.178.218', user='gh', password='a12345', database='wagodb')


def load(since):
    con = pymysql.connect(**DB, cursorclass=pymysql.cursors.DictCursor, connect_timeout=5)
    with con.cursor() as c:
        c.execute("""
            SELECT ts, compressor_actual_frequency hz, compressor_operating_power p,
                   ambient_temperature t4, defrosting_active d, water_tank_temperature tank,
                   outlet_water_temperature outlet, mode
            FROM heatpr290 WHERE ts >= %s ORDER BY ts""", (since,))
        rows = c.fetchall()
    con.close()
    return rows


def cycles(rows):
    out, cur = [], None
    for r in rows:
        if float(r['hz'] or 0) > 0:
            if cur and r['ts'] - cur[-1]['ts'] > timedelta(minutes=5):
                out.append(cur)
                cur = None
            cur = (cur or []) + [r]
        elif cur:
            out.append(cur)
            cur = None
    if cur:
        out.append(cur)
    return out


def f(v):
    return None if v is None or v == '' else float(v)


def describe(z, prev_end):
    run = (z[-1]['ts'] - z[0]['ts']).total_seconds() / 60 + 1
    hz = [f(r['hz']) for r in z if not int(r['d'] or 0)]
    t4 = [f(r['t4']) for r in z if f(r['t4']) is not None]
    out = [f(r['outlet']) for r in z if f(r['outlet']) is not None]
    return {
        'start': z[0]['ts'], 'end': z[-1]['ts'], 'run': run,
        'pause': (z[0]['ts'] - prev_end).total_seconds() / 60 - 1 if prev_end else None,
        'defrost': sum(1 for r in z if int(r['d'] or 0)),
        'hz': st.mean(hz) if hz else 0,
        'kwh': sum(f(r['p']) or 0 for r in z) * 100 / 60 / 1000,
        'tank0': f(z[0]['tank']), 'tank1': f(z[-1]['tank']),
        'outlet_max': max(out) if out else None,
        't4': st.mean(t4) if t4 else None,
        'mode': z[0]['mode'],
        'open': False,
    }


def fmt(v, w, d=1):
    return f"{v:{w}.{d}f}" if v is not None else ' ' * (w - 1) + '–'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=float, default=2, help='evaluation window in days')
    ap.add_argument('--short', type=float, default=20, help='runs shorter than this many minutes count as short')
    a = ap.parse_args()
    now = datetime.now()
    since = now - timedelta(days=a.days)
    rows = load(since)
    zs = cycles(rows)
    if not zs:
        print(f"=== rep_cycles {now:%Y-%m-%d %H:%M} === keine Verdichterlaeufe seit {since:%d.%m. %H:%M}")
        return
    res, prev_end = [], None
    for z in zs:
        res.append(describe(z, prev_end))
        prev_end = z[-1]['ts']
    # a run touching the window start or still running now is cut off
    res[0]['open'] = zs[0][0]['ts'] - rows[0]['ts'] < timedelta(minutes=2)
    res[-1]['open'] = res[-1]['open'] or rows[-1]['ts'] == zs[-1][-1]['ts']

    print(f"=== rep_cycles {now:%Y-%m-%d %H:%M} === letzte {a.days:g} Tage ab {since:%d.%m. %H:%M}, "
          f"{len(res)} Verdichterlaeufe (* = am Fensterrand angeschnitten)")
    print("\nStart        Ende    Lauf  Pause  Abt   Hz  kWh_el  Tank Start->Ende  VL max    T4  Mode")
    for c in res:
        print(f"{c['start']:%a %H:%M}  {c['end']:%H:%M} {c['run']:5.0f}{'*' if c['open'] else ' '} "
              f"{fmt(c['pause'], 5, 0)} {c['defrost']:4d} {c['hz']:4.0f} {c['kwh']:7.2f}  "
              f"{fmt(c['tank0'], 5)} -> {fmt(c['tank1'], 5)}   {fmt(c['outlet_max'], 5)} {fmt(c['t4'], 5)}  {c['mode']}")

    print(f"\nTag          Starts  Lauf h  Anteil  Median Lauf  Median Pause  < {a.short:g} min  kWh_el  T4 Mittel")
    days = {}
    for c in res:
        days.setdefault(c['start'].date(), []).append(c)
    for d, cs in days.items():
        d0 = datetime.combine(d, datetime.min.time())
        span_end = min(now, d0 + timedelta(days=1))
        span = (span_end - max(since, d0)).total_seconds() / 60
        run = sum(c['run'] for c in cs)
        pauses = [c['pause'] for c in cs if c['pause'] is not None]
        t4 = [c['t4'] for c in cs if c['t4'] is not None]
        print(f"{d:%a %d.%m.}  {len(cs):9d} {run / 60:7.1f} {100 * run / span:6.0f} % {st.median(c['run'] for c in cs):9.0f} min"
              f" {fmt(st.median(pauses) if pauses else None, 9, 0)} min {sum(1 for c in cs if c['run'] < a.short):10d}"
              f" {sum(c['kwh'] for c in cs):8.1f} {fmt(st.mean(t4) if t4 else None, 9)}")
    print("Anteil = Laufminuten / betrachtete Minuten des Tages (erster und heutiger Tag nur teilweise im Fenster)")


if __name__ == '__main__':
    main()
