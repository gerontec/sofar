#!/usr/bin/env python3
"""
PV DC-Leistung (Power_PV1 + Power_PV2) mit Sonnenstand-Overlay.
Aufruf: python3 pv_solar_plot.py [YYYY-MM-DD]  (default: heute)
"""

import sys
import datetime as dt
import pymysql
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from astral import LocationInfo
from astral.sun import sun, elevation

LAT, LON = 47.6811, 11.5732
TZ = "Europe/Berlin"
DB = dict(host="192.168.178.218", db="wagodb", user="gh", password="a12345")


def fetch_pv(date: dt.date) -> tuple[list, list]:
    conn = pymysql.connect(**DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT timestamp, (Power_PV1 + Power_PV2) * 1000
        FROM inverter_data
        WHERE DATE(timestamp) = %s
          AND Power_PV1 IS NOT NULL
          AND Power_PV2 IS NOT NULL
        ORDER BY timestamp
    """, (date,))
    rows = cur.fetchall()
    conn.close()
    times = [r[0] for r in rows]
    watts = [r[1] for r in rows]
    return times, watts


def solar_curve(date: dt.date) -> tuple[list, list]:
    loc = LocationInfo("Lenggries", "Germany", TZ, LAT, LON)
    import zoneinfo
    tz = zoneinfo.ZoneInfo(TZ)
    times, elevs = [], []
    t = dt.datetime(date.year, date.month, date.day, 4, 0, tzinfo=tz)
    end = dt.datetime(date.year, date.month, date.day, 22, 0, tzinfo=tz)
    while t <= end:
        elev = elevation(loc.observer, t)
        times.append(t.replace(tzinfo=None))
        elevs.append(max(elev, 0))
        t += dt.timedelta(minutes=5)
    return times, elevs


def solar_noon_time(date: dt.date) -> dt.datetime:
    import zoneinfo
    loc = LocationInfo("Lenggries", "Germany", TZ, LAT, LON)
    tz = zoneinfo.ZoneInfo(TZ)
    s = sun(loc.observer, date=date, tzinfo=tz)
    return s["noon"].replace(tzinfo=None)


def main():
    date = dt.date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else dt.date.today()
    print(f"Lade Daten für {date} …")

    pv_times, pv_watts = fetch_pv(date)
    sol_times, sol_elevs = solar_curve(date)
    noon = solar_noon_time(date)

    if not pv_times:
        print("Keine PV-Daten für dieses Datum gefunden.")
        sys.exit(1)

    pv_max_w = max(pv_watts)
    pv_max_t = pv_times[pv_watts.index(pv_max_w)]
    sol_max_e = max(sol_elevs)
    diff_min = (pv_max_t - noon).total_seconds() / 60

    print(f"Solar Noon:    {noon.strftime('%H:%M')} CEST")
    print(f"PV Peak:       {pv_max_t.strftime('%H:%M')} CEST  ({pv_max_w:.0f} W)")
    print(f"Versatz:       {diff_min:+.0f} min")

    fig, ax1 = plt.subplots(figsize=(14, 6))
    fig.patch.set_facecolor("#1a1a2e")
    ax1.set_facecolor("#16213e")

    # PV-Leistung
    ax1.fill_between(pv_times, pv_watts, alpha=0.4, color="#f5a623", label="PV DC (W)")
    ax1.plot(pv_times, pv_watts, color="#f5a623", linewidth=1.5)
    ax1.set_ylabel("PV DC-Leistung (W)", color="#f5a623", fontsize=11)
    ax1.tick_params(axis="y", labelcolor="#f5a623")
    ax1.set_ylim(bottom=0)

    # Sonnenstand (rechte Achse)
    ax2 = ax1.twinx()
    ax2.plot(sol_times, sol_elevs, color="#87ceeb", linewidth=2,
             linestyle="--", alpha=0.8, label="Sonnenstand (°)")
    ax2.set_ylabel("Sonnenhöhe (°)", color="#87ceeb", fontsize=11)
    ax2.tick_params(axis="y", labelcolor="#87ceeb")
    ax2.set_ylim(0, sol_max_e * 1.3)

    # Solar Noon Linie
    ax1.axvline(noon, color="#ffffff", linestyle=":", linewidth=1.2, alpha=0.6)
    ax1.text(noon, ax1.get_ylim()[1] * 0.97,
             f"Noon\n{noon.strftime('%H:%M')}",
             color="#ffffff", fontsize=8, ha="center", va="top", alpha=0.7)

    # PV Peak Marker
    ax1.axvline(pv_max_t, color="#f5a623", linestyle=":", linewidth=1.2, alpha=0.6)
    ax1.annotate(
        f"PV-Peak\n{pv_max_t.strftime('%H:%M')}\n{pv_max_w:.0f} W\n({diff_min:+.0f} min)",
        xy=(pv_max_t, pv_max_w),
        xytext=(15, -40), textcoords="offset points",
        color="#f5a623", fontsize=8,
        arrowprops=dict(arrowstyle="->", color="#f5a623", lw=1),
    )

    # Formatierung
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax1.xaxis.set_major_locator(mdates.HourLocator(interval=1))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha="right")

    for spine in ax1.spines.values():
        spine.set_edgecolor("#444")
    for spine in ax2.spines.values():
        spine.set_edgecolor("#444")
    ax1.tick_params(colors="#ccc")
    ax2.tick_params(colors="#ccc")
    ax1.xaxis.label.set_color("#ccc")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2,
               loc="upper left", facecolor="#1a1a2e", edgecolor="#444",
               labelcolor="white", fontsize=9)

    ax1.set_title(
        f"PV DC-Leistung + Sonnenstand — {date.strftime('%d.%m.%Y')}  |  "
        f"Lenggries (47.68°N, 11.57°E)\n"
        f"Solar Noon: {noon.strftime('%H:%M')} CEST  |  "
        f"PV-Peak: {pv_max_t.strftime('%H:%M')} CEST  |  Versatz: {diff_min:+.0f} min",
        color="white", fontsize=11, pad=10
    )
    ax1.grid(True, color="#333", linestyle="--", linewidth=0.5)

    outfile = f"pv_solar_{date}.png"
    plt.tight_layout()
    plt.savefig(outfile, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    print(f"Gespeichert: {outfile}")
    plt.show()


if __name__ == "__main__":
    main()
