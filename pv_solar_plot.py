#!/usr/bin/env python3
"""
Three stacked subplots for one day: solar elevation, PV DC power, PCC grid feed-in.
Usage: python3 pv_solar_plot.py [YYYY-MM-DD]  (default: today)
"""

import sys
import datetime as dt
import zoneinfo
import pymysql
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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
          AND Power_PV1 IS NOT NULL AND Power_PV2 IS NOT NULL
        ORDER BY timestamp
    """, (date,))
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows], [float(r[1]) for r in rows]


def fetch_pcc(date: dt.date) -> tuple[list, list]:
    conn = pymysql.connect(**DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT timestamp, ActivePower_PCC_Total * 1000
        FROM inverter_data
        WHERE DATE(timestamp) = %s
          AND ActivePower_PCC_Total IS NOT NULL
          AND ActivePower_PCC_Total != 0
        ORDER BY timestamp
    """, (date,))
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows], [float(r[1]) for r in rows]


def solar_curve(date: dt.date) -> tuple[list, list]:
    loc = LocationInfo("Lenggries", "Germany", TZ, LAT, LON)
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
    loc = LocationInfo("Lenggries", "Germany", TZ, LAT, LON)
    tz = zoneinfo.ZoneInfo(TZ)
    s = sun(loc.observer, date=date, tzinfo=tz)
    return s["noon"].replace(tzinfo=None)


def style_ax(ax, ylabel, color):
    ax.set_facecolor("#16213e")
    ax.set_ylabel(ylabel, color=color, fontsize=10)
    ax.tick_params(axis="y", labelcolor=color)
    ax.tick_params(axis="x", colors="#ccc")
    ax.grid(True, color="#333", linestyle="--", linewidth=0.5)
    for spine in ax.spines.values():
        spine.set_edgecolor("#444")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))


def add_noon_line(ax, noon, ymax):
    ax.axvline(noon, color="#ffffff", linestyle=":", linewidth=1.0, alpha=0.5)
    ax.text(noon, ymax * 0.97, f"Noon\n{noon.strftime('%H:%M')}",
            color="#ffffff", fontsize=7, ha="center", va="top", alpha=0.6)


def main():
    date = dt.date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else dt.date.today()
    print(f"Loading data for {date} …")

    pv_times, pv_watts = fetch_pv(date)
    pcc_times, pcc_watts = fetch_pcc(date)
    sol_times, sol_elevs = solar_curve(date)
    noon = solar_noon_time(date)

    if not pv_times:
        print("No PV data found for this date.")
        sys.exit(1)

    pv_max_w = max(pv_watts)
    pv_max_t = pv_times[pv_watts.index(pv_max_w)]
    diff_min = (pv_max_t - noon).total_seconds() / 60
    pcc_max_w = max(pcc_watts) if pcc_watts else 0

    print(f"Solar Noon:  {noon.strftime('%H:%M')} CEST")
    print(f"PV Peak:     {pv_max_t.strftime('%H:%M')} CEST  ({pv_max_w:.0f} W)  offset {diff_min:+.0f} min")
    print(f"PCC Peak:    {pcc_max_w:.0f} W")

    fig, (ax_sun, ax_pv, ax_pcc) = plt.subplots(3, 1, figsize=(14, 11), sharex=True)
    fig.patch.set_facecolor("#1a1a2e")
    fig.suptitle(
        f"Solar elevation · PV DC · Grid feed-in (PCC) — {date.strftime('%d %b %Y')}  |  "
        f"Lenggries (47.68°N, 11.57°E)\n"
        f"Solar Noon: {noon.strftime('%H:%M')} CEST  |  "
        f"PV Peak: {pv_max_t.strftime('%H:%M')} ({pv_max_w:.0f} W, offset {diff_min:+.0f} min)  |  "
        f"PCC Peak: {pcc_max_w:.0f} W",
        color="white", fontsize=11, y=0.98
    )

    # ── Panel 1: Solar elevation ─────────────────────────────────────────
    style_ax(ax_sun, "Solar elevation (°)", "#87ceeb")
    ax_sun.fill_between(sol_times, sol_elevs, alpha=0.3, color="#87ceeb")
    ax_sun.plot(sol_times, sol_elevs, color="#87ceeb", linewidth=1.8)
    ax_sun.set_ylim(bottom=0)
    add_noon_line(ax_sun, noon, max(sol_elevs) * 1.05)

    # ── Panel 2: PV DC power ─────────────────────────────────────────────
    style_ax(ax_pv, "PV DC power (W)", "#f5a623")
    ax_pv.fill_between(pv_times, pv_watts, alpha=0.4, color="#f5a623")
    ax_pv.plot(pv_times, pv_watts, color="#f5a623", linewidth=1.5)
    ax_pv.set_ylim(bottom=0)
    add_noon_line(ax_pv, noon, pv_max_w * 1.05)
    ax_pv.annotate(
        f"{pv_max_t.strftime('%H:%M')}\n{pv_max_w:.0f} W\n({diff_min:+.0f} min)",
        xy=(pv_max_t, pv_max_w),
        xytext=(15, -40), textcoords="offset points",
        color="#f5a623", fontsize=8,
        arrowprops=dict(arrowstyle="->", color="#f5a623", lw=1),
    )

    # ── Panel 3: PCC grid feed-in ────────────────────────────────────────
    style_ax(ax_pcc, "Grid feed-in PCC (W)", "#e05c5c")
    if pcc_watts:
        ax_pcc.fill_between(pcc_times, pcc_watts, alpha=0.4, color="#e05c5c")
        ax_pcc.plot(pcc_times, pcc_watts, color="#e05c5c", linewidth=1.5)
        ax_pcc.axhline(20_000, color="#ffffff", linestyle="--", linewidth=1.0,
                       alpha=0.6, label="20 kW limit")
        ax_pcc.axhline(19_000, color="#aaaaaa", linestyle=":", linewidth=1.0,
                       alpha=0.5, label="19 kW FeedInLimiter threshold")
        ax_pcc.legend(loc="upper left", facecolor="#1a1a2e", edgecolor="#444",
                      labelcolor="white", fontsize=8)
        ax_pcc.set_ylim(bottom=0)
        add_noon_line(ax_pcc, noon, max(pcc_watts) * 1.05)
    else:
        ax_pcc.text(0.5, 0.5, "No PCC data", transform=ax_pcc.transAxes,
                    color="#ccc", ha="center", va="center")

    plt.setp(ax_pcc.xaxis.get_majorticklabels(), rotation=45, ha="right")
    ax_pcc.set_xlabel("Time (CEST)", color="#ccc", fontsize=10)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    outfile = f"pv_solar_{date}.png"
    plt.savefig(outfile, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    print(f"Saved: {outfile}")
    plt.show()


if __name__ == "__main__":
    main()
