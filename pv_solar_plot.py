#!/usr/bin/env python3
"""
Overlaid plot for one day: solar elevation, PV DC power, PCC grid feed-in.
Usage: python3 pv_solar_plot.py [YYYYMMDD]  (default: today)

Key annotations:
  • Morning onset  — first PV DC > 500 W
  • Solar noon     — elevation peak
  • Current time   — elevation + watts right now (today only)
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
TZ       = "Europe/Berlin"
DB       = dict(host="192.168.178.218", db="wagodb", user="gh", password="a12345")
_LOC     = LocationInfo("Lenggries", "Germany", TZ, LAT, LON)
_TZ      = zoneinfo.ZoneInfo(TZ)


# ── Helpers ──────────────────────────────────────────────────────────────────

def elev_at(t: dt.datetime) -> float:
    """Solar elevation (°) at naive or aware datetime."""
    t_aware = t.replace(tzinfo=_TZ) if t.tzinfo is None else t
    return elevation(_LOC.observer, t_aware)


def closest_idx(times: list, target: dt.datetime) -> int:
    return min(range(len(times)), key=lambda i: abs((times[i] - target).total_seconds()))


# ── DB queries ────────────────────────────────────────────────────────────────

def fetch_pv(date: dt.date) -> tuple[list, list]:
    conn = pymysql.connect(**DB)
    cur  = conn.cursor()
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
    cur  = conn.cursor()
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


# ── Solar curve ───────────────────────────────────────────────────────────────

def solar_curve(date: dt.date) -> tuple[list, list]:
    times, elevs = [], []
    t   = dt.datetime(date.year, date.month, date.day,  4, 0, tzinfo=_TZ)
    end = dt.datetime(date.year, date.month, date.day, 22, 0, tzinfo=_TZ)
    while t <= end:
        times.append(t.replace(tzinfo=None))
        elevs.append(max(elev_at(t), 0))
        t += dt.timedelta(minutes=5)
    return times, elevs


def solar_noon_time(date: dt.date) -> dt.datetime:
    s = sun(_LOC.observer, date=date, tzinfo=_TZ)
    return s["noon"].replace(tzinfo=None)


# ── Axes styling ──────────────────────────────────────────────────────────────

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


# ── Key-point annotation ──────────────────────────────────────────────────────

def annotate_keypoint(ax_pv, t: dt.datetime, pv_w: float,
                      label: str, color: str, y_frac: float = 0.92):
    """
    Vertikale Linie + eine Box im oberen Leerraum (y_frac = Achsenfraktion).
    Box zeigt: Label, Uhrzeit, Sonnenwinkel, Watt.
    """
    elev = elev_at(t)
    kw   = pv_w / 1000
    text = f"{label}\n↑ {elev:.1f}°  {kw:.2f} kW"

    ax_pv.axvline(t, color=color, linestyle=":", linewidth=1.1, alpha=0.75)
    ax_pv.annotate(
        text,
        xy=(t, pv_w),
        xytext=(t, y_frac),
        xycoords="data",
        textcoords=("data", "axes fraction"),
        color=color, fontsize=8, ha="center", va="top",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="#1a1a2e",
                  edgecolor=color, alpha=0.92, linewidth=1.2),
        arrowprops=dict(arrowstyle="->", color=color, lw=0.9,
                        connectionstyle="arc3,rad=0.0"),
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) > 1:
        arg = sys.argv[1].replace("-", "")          # accept YYYYMMDD or YYYY-MM-DD
        date = dt.date(int(arg[:4]), int(arg[4:6]), int(arg[6:8]))
    else:
        date = dt.date.today()
    is_today = (date == dt.date.today())
    print(f"Loading data for {date} …")

    pv_times, pv_watts   = fetch_pv(date)
    pcc_times, pcc_watts = fetch_pcc(date)
    sol_times, sol_elevs = solar_curve(date)
    noon                 = solar_noon_time(date)

    if not pv_times:
        print("No PV data found for this date.")
        sys.exit(1)

    pv_max_w = max(pv_watts)
    pv_max_t = pv_times[pv_watts.index(pv_max_w)]
    diff_min = (pv_max_t - noon).total_seconds() / 60
    pcc_max_w = max(pcc_watts) if pcc_watts else 0

    # ── Key points ────────────────────────────────────────────────────────────

    # 1) Morning onset: first PV > 500 W
    morning_t = morning_w = None
    for t, w in zip(pv_times, pv_watts):
        if w > 500:
            morning_t, morning_w = t, w
            break

    # 2) Noon: PV watts at solar noon
    noon_idx = closest_idx(pv_times, noon)
    noon_w   = pv_watts[noon_idx]
    noon_elev = elev_at(noon)

    # 3) Evening: last PV > 500 W
    evening_t = evening_w = None
    for t, w in zip(reversed(pv_times), reversed(pv_watts)):
        if w > 500:
            evening_t, evening_w = t, w
            break

    # 4) Current time (today only)
    now_t = now_w = None
    if is_today:
        now_naive = dt.datetime.now().replace(second=0, microsecond=0)
        if pv_times[0] <= now_naive <= pv_times[-1]:
            now_idx = closest_idx(pv_times, now_naive)
            now_t   = pv_times[now_idx]
            now_w   = pv_watts[now_idx]

    print(f"Solar Noon:      {noon.strftime('%H:%M')} CEST  ↑ {noon_elev:.1f}°  {noon_w/1000:.2f} kW")
    print(f"PV Peak:         {pv_max_t.strftime('%H:%M')} CEST  {pv_max_w:.0f} W  (offset {diff_min:+.0f} min)")
    if morning_t:
        print(f"Morning onset:   {morning_t.strftime('%H:%M')} CEST  ↑ {elev_at(morning_t):.1f}°  {morning_w/1000:.2f} kW")
    if evening_t:
        print(f"Evening cutoff:  {evening_t.strftime('%H:%M')} CEST  ↑ {elev_at(evening_t):.1f}°  {evening_w/1000:.2f} kW")
    if now_t:
        print(f"Jetzt:           {now_t.strftime('%H:%M')} CEST  ↑ {elev_at(now_t):.1f}°  {now_w/1000:.2f} kW")
    print(f"PCC Peak:        {pcc_max_w:.0f} W")

    # ── Figure — alle 3 Kurven überlagert ────────────────────────────────────

    fig, ax_pv = plt.subplots(1, 1, figsize=(14, 7))
    fig.patch.set_facecolor("#1a1a2e")
    ax_sun = ax_pv.twinx()                        # rechts: Solar elevation
    ax_pcc = ax_pv.twinx()                        # rechts (versetzt): PCC
    ax_pcc.spines["right"].set_position(("axes", 1.07))

    fig.suptitle(
        f"Solar elevation · PV DC · Grid feed-in (PCC) — {date.strftime('%d %b %Y')}  |  "
        f"Lenggries (47.68°N, 11.57°E)\n"
        f"Solar Noon: {noon.strftime('%H:%M')} CEST  ↑ {noon_elev:.1f}°  |  "
        f"PV Peak: {pv_max_t.strftime('%H:%M')} ({pv_max_w:.0f} W, offset {diff_min:+.0f} min)  |  "
        f"PCC Peak: {pcc_max_w:.0f} W",
        color="white", fontsize=11, y=0.99
    )

    ax_pv.set_facecolor("#16213e")
    for ax in (ax_pv, ax_sun, ax_pcc):
        ax.tick_params(axis="x", colors="#ccc")
        for spine in ax.spines.values():
            spine.set_edgecolor("#444")
    ax_pv.grid(True, color="#333", linestyle="--", linewidth=0.5)
    ax_pv.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax_pv.xaxis.set_major_locator(mdates.HourLocator(interval=1))

    sun_ymax = max(sol_elevs) * 1.05
    pv_ymax  = pv_max_w * 1.05

    # Solar elevation (links hinter den anderen Kurven)
    ax_sun.fill_between(sol_times, sol_elevs, alpha=0.15, color="#87ceeb")
    ax_sun.plot(sol_times, sol_elevs, color="#87ceeb", linewidth=1.6, alpha=0.7)
    ax_sun.set_ylim(bottom=0, top=sun_ymax * 1.4)
    ax_sun.set_ylabel("Solar elevation (°)", color="#87ceeb", fontsize=10)
    ax_sun.tick_params(axis="y", labelcolor="#87ceeb")

    # PV DC (Hauptachse links) — fixes Maximum 33 kW (theoretisches Anlagen-Maximum)
    ax_pv.fill_between(pv_times, pv_watts, alpha=0.45, color="#f5a623")
    ax_pv.plot(pv_times, pv_watts, color="#f5a623", linewidth=1.8)
    ax_pv.set_ylim(0, 33_000)
    ax_pv.set_ylabel("PV DC power (W)", color="#f5a623", fontsize=10)
    ax_pv.tick_params(axis="y", labelcolor="#f5a623")

    # PCC (zweite rechte Achse)
    if pcc_watts:
        ax_pcc.fill_between(pcc_times, pcc_watts, alpha=0.35, color="#e05c5c")
        ax_pcc.plot(pcc_times, pcc_watts, color="#e05c5c", linewidth=1.5)
        ax_pcc.axhline(20_000, color="#ffffff", linestyle="--", linewidth=0.9,
                       alpha=0.5, label="20 kW limit")
        ax_pcc.axhline(19_000, color="#aaaaaa", linestyle=":", linewidth=0.9,
                       alpha=0.45, label="19 kW FeedIn")
        ax_pcc.set_ylim(0, 33_000)
    ax_pcc.set_ylabel("Grid feed-in PCC (W)", color="#e05c5c", fontsize=10)
    ax_pcc.tick_params(axis="y", labelcolor="#e05c5c")

    # Noon-Linie
    ax_pv.axvline(noon, color="#ffffff", linestyle=":", linewidth=1.0, alpha=0.4)

    # ── Key-point annotations (Boxen im oberen Leerraum) ─────────────────────
    # Reihe 1 (y=0.97): Morgen · Noon · Abend  — zeitlich gespreizt, keine Überlappung
    if morning_t:
        annotate_keypoint(ax_pv, morning_t, morning_w,
                          f"Morgen {morning_t.strftime('%H:%M')}",
                          "#7ec8e3", y_frac=0.97)

    annotate_keypoint(ax_pv, noon, noon_w,
                      f"Noon {noon.strftime('%H:%M')}",
                      "#ffffff", y_frac=0.97)

    if evening_t:
        annotate_keypoint(ax_pv, evening_t, evening_w,
                          f"Abend {evening_t.strftime('%H:%M')}",
                          "#f0a0ff", y_frac=0.97)

    # Reihe 2 (y=0.84): DC-Max mit Versatz zu Noon
    sign = "+" if diff_min >= 0 else ""
    annotate_keypoint(ax_pv, pv_max_t, pv_max_w,
                      f"DC-Max {pv_max_t.strftime('%H:%M')}  ({sign}{diff_min:.0f} min)",
                      "#f5c842", y_frac=0.84)

    # Reihe 3 (y=0.71): Jetzt (nur heute)
    if now_t:
        annotate_keypoint(ax_pv, now_t, now_w,
                          f"Jetzt {now_t.strftime('%H:%M')}",
                          "#00e676", y_frac=0.71)

    # Legende
    from matplotlib.lines import Line2D
    legend_items = [
        Line2D([0], [0], color="#87ceeb", linewidth=2, label="Solar elevation (°)"),
        Line2D([0], [0], color="#f5a623", linewidth=2, label="PV DC (W)"),
        Line2D([0], [0], color="#e05c5c", linewidth=2, label="Grid feed-in PCC (W)"),
        Line2D([0], [0], color="#ffffff", linestyle="--", linewidth=1, label="20 kW limit"),
        Line2D([0], [0], color="#aaaaaa", linestyle=":",  linewidth=1, label="19 kW FeedIn"),
    ]
    ax_pv.legend(handles=legend_items, loc="upper left",
                 facecolor="#1a1a2e", edgecolor="#444", labelcolor="white", fontsize=8)

    plt.setp(ax_pv.xaxis.get_majorticklabels(), rotation=45, ha="right")
    ax_pv.set_xlabel("Time (CEST)", color="#ccc", fontsize=10)

    plt.tight_layout(rect=[0, 0, 0.93, 0.95])
    outfile = f"pv_solar_{date}.png"
    plt.savefig(outfile, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    print(f"Saved: {outfile}")
    plt.show()


if __name__ == "__main__":
    main()
