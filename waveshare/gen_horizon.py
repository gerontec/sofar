#!/usr/bin/python3
"""Horizontprofil der Ostseite aus den FoxESS-Messwerten herleiten.

Das Clear-Sky-Modell kennt keinen Horizont. Verschattung zeigt sich deshalb als
Morgendefizit: die Sonne steht rechnerisch laengst ueber dem Horizont, die
Anlage liefert aber noch nichts. Der Zeitpunkt, an dem die Messung auf ihr
normales Niveau springt, ist die Verschattungskante - Sonnenazimut und
-elevation dort ergeben einen Punkt des Horizontprofils.

Bezugsniveau ist der Median von ist/Modell zwischen 11 und 13 Uhr desselben
Tages. Damit faellt der bekannte Betragsfehler des Modells (Monatsfaktor kt)
und die Tagesguete heraus, es zaehlt nur die Form des Vormittags.

Rein lesend. Sinnvoll nur fuer klare Tage.
"""

import argparse
import datetime as dt
import math
import statistics
import sys
import time

sys.path.insert(0, '/home/pi/python')
import fox2dbEasy as fx                    # noqa: E402
from db_config import get_db_connection    # noqa: E402

EDGE = 0.5          # Anteil des Mittagsniveaus, ab dem "unverschattet" gilt
MIN_MOD_W = 300.0   # darunter ist das Modell selbst zu unsicher


def day_points(cur, day):
    cur.execute("""SELECT timestamp, pvPower*1000 FROM inverter_data2
                    WHERE DATE(timestamp) = %s AND pvPower IS NOT NULL
                    ORDER BY timestamp""", (day,))
    out = []
    for ts, p in cur.fetchall():
        t_utc = time.mktime(ts.timetuple())
        elev, azN = fx.sun_pos(t_utc)
        if elev <= 0:
            continue
        mod = fx.calc_arrays(fx.ARRAYS_EAST, t_utc, ts.month)
        out.append((ts, float(p), mod, elev, azN))
    return out


def analyse(day, pts):
    noon = [p / m for ts, p, m, _, _ in pts if 11 <= ts.hour < 13 and m > MIN_MOD_W]
    if not noon:
        return None
    ref = statistics.median(noon)
    if ref <= 0:
        return None

    sunrise = None          # erster Punkt mit nennenswertem Modellwert
    edge = None
    # Weder die erste Ueberschreitung noch die letzte Unterschreitung taugen:
    # bei tiefer Sonne reisst ein einzelner Rauschwert die Schwelle nach oben
    # (03.09.2025: 0.73 bei 311 W Modell, danach 0.26), eine spaete Wolke zieht
    # sie nochmal nach unten (23.04.2026: Kante erst bei 46 Grad Elevation).
    # Massgeblich ist die erste ANHALTENDE Ueberschreitung.
    HOLD = 5                                     # 5 Punkte a 3 min = 15 min
    seq = []
    for ts, p, mod, elev, azN in pts:
        if mod < MIN_MOD_W:
            continue
        if sunrise is None:
            sunrise = (ts, elev, azN)
        if ts.hour >= 12:
            break
        seq.append((ts, p, mod, elev, azN))
    for i in range(len(seq) - HOLD + 1):
        if all(q / m >= EDGE * ref for _, q, m, _, _ in seq[i:i + HOLD]):
            ts, p, mod, elev, azN = seq[i]
            edge = (ts, elev, azN)
            break

    lost = sum((m * ref - p) for ts, p, m, _, _ in pts
               if edge and ts < edge[0] and m > MIN_MOD_W) / 20.0 / 1000.0  # 3-min -> kWh
    total = sum(p for _, p, _, _, _ in pts) / 20.0 / 1000.0
    return dict(day=day, ref=ref, sunrise=sunrise, edge=edge,
                lost_kwh=max(0.0, lost), total_kwh=total)


def main():
    ap = argparse.ArgumentParser(description="Horizontprofil Ost aus Messdaten")
    ap.add_argument("days", nargs="*", help="Tage YYYY-MM-DD (ohne Angabe: alle in der DB)")
    args = ap.parse_args()

    conn = get_db_connection()
    cur = conn.cursor()
    if args.days:
        days = [dt.date.fromisoformat(d) for d in args.days]
    else:
        cur.execute("SELECT DISTINCT DATE(timestamp) FROM inverter_data2 ORDER BY 1")
        days = [r[0] for r in cur.fetchall()]

    print("%-11s %8s   %-14s %-19s %7s %8s %7s %6s" %
          ("Tag", "Niveau", "Sonne ueber 0", "Ertrag setzt ein", "Verzug",
           "Ertrag", "fehlend", "Verlust"))
    print("%-11s %8s   %-14s %-19s %7s %8s %7s %6s" %
          ("", "ist/mod", "Zeit  elev/az", "Zeit  elev/az", "min", "kWh", "kWh", "%"))
    print("-" * 92)
    rows = []
    for d in days:
        pts = day_points(cur, d)
        if len(pts) < 30:
            continue
        r = analyse(d, pts)
        if not r or not r["edge"] or not r["sunrise"]:
            continue
        rows.append(r)
        s_ts, s_el, s_az = r["sunrise"]
        e_ts, e_el, e_az = r["edge"]
        share = 100.0 * r["lost_kwh"] / (r["lost_kwh"] + r["total_kwh"]) \
            if (r["lost_kwh"] + r["total_kwh"]) > 0 else 0.0
        print("%-11s %8.2f   %s %2.0f/%3.0f     %s %2.0f/%3.0f    %5.0f %8.1f %7.1f %6.0f" % (
            d, r["ref"], s_ts.strftime("%H:%M"), s_el, s_az,
            e_ts.strftime("%H:%M"), e_el, e_az,
            (e_ts - s_ts).total_seconds() / 60.0, r["total_kwh"], r["lost_kwh"], share))
    conn.close()

    if rows:
        print()
        print("Horizontprofil Ost (Sonnenazimut -> blockierende Elevation):")
        # Tage ohne nennenswerten Verzug tragen keinen Horizontpunkt bei
        for r in sorted((x for x in rows
                         if (x["edge"][0] - x["sunrise"][0]).total_seconds() > 1200),
                        key=lambda x: x["edge"][2]):
            print("   Azimut %3.0f°  ->  bis %2.0f° Elevation verschattet   (%s)"
                  % (r["edge"][2], r["edge"][1], r["day"]))


if __name__ == "__main__":
    main()
