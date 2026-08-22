#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════════════
# expectsoc100.py — Prognose des hoechsten heute erreichbaren EBox-SOC (30 kWh)
# und der Uhrzeit dazu: "expect max xx % at hh:mm".
#
# Datenbasis (alles Trend, nichts geraten):
#   - SOC + Ladeleistung: wagodb.pv_decision_log (version='waveshare', 60s-Takt,
#     soc = soc2 = EBox, ebox_w = AC-Ladeleistung)
#   - kWh-pro-Prozent: aus dem Messfenster selbst gerechnet (Energie/ΔSOC),
#     Fallback K_DEFAULT wenn das Fenster zu verrauscht ist
#   - Klarhimmel-Restertrag: _DcForecast 1:1 aus fox2db.py geladen (ast), damit
#     es keine zweite Sonnenstandsrechnung gibt, die auseinanderlaufen kann
#   - Bewölkungsfaktor: Ist/Klarhimmel der letzten Minuten (gleicher Proxy wie
#     die ESP-Ratio: ist = pcc + ebox + bat1)
#
# Aufruf:  ./expectsoc100.py [--window 20] [--target 100] [--json]
# ═══════════════════════════════════════════════════════════════════════════
import argparse
import ast
import datetime as dt
import json
import math
import pathlib
import sys
import zoneinfo

import pymysql
from astral import LocationInfo as _LocationInfo
from astral.sun import elevation as _astral_elevation, azimuth as _astral_azimuth

DB_CFG    = {'host': '192.168.178.218', 'db': 'wagodb', 'user': 'gh', 'pw': 'a12345'}
FOX2DB    = pathlib.Path(__file__).with_name('fox2db.py')
VERSION   = 'v1.0'

K_DEFAULT = 0.32      # kWh(AC) je SOC-Prozent (30-kWh-EBox, empirisch 19.08.2026)
K_RANGE   = (0.20, 0.50)   # plausibler Bereich fuer den gemessenen K-Wert
LOAD_W    = 800       # Grundlast, die vor der EBox bedient wird
PMAX_W    = 11400     # Stufe 7 (STATE_TO_POWER[7] in fox2db.py)
END_HOUR  = 21        # Rechenhorizont (nach Sonnenuntergang)


# ── Klarhimmel-Modell aus fox2db.py ziehen ──────────────────────────────────
def load_dc_forecast():
    """_DcForecast unveraendert aus fox2db.py holen (fox2db selbst importiert
    paho/pymysql und ist nicht ueberall importierbar)."""
    tree = ast.parse(FOX2DB.read_text())
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == '_DcForecast':
            ns = {'math': math, 'dt': dt, 'zoneinfo': zoneinfo,
                  '_LocationInfo': _LocationInfo,
                  '_astral_elevation': _astral_elevation,
                  '_astral_azimuth': _astral_azimuth}
            exec(compile(ast.Module(body=[node], type_ignores=[]),
                         str(FOX2DB), 'exec'), ns)
            return ns['_DcForecast']()
    sys.exit(f"_DcForecast nicht in {FOX2DB} gefunden")


def clearsky_w(dc, t_local):
    """Klarhimmel-DC beider Array-Gruppen zum lokalen Zeitpunkt t_local."""
    naive = t_local.replace(tzinfo=None)
    return dc._calc(dc._ARRAYS, naive) + dc._calc(dc._ARRAYS_EAST, naive)


# ── Trend aus der DB ────────────────────────────────────────────────────────
def fetch_trend(window_min):
    conn = pymysql.connect(host=DB_CFG['host'], database=DB_CFG['db'],
                           user=DB_CFG['user'], password=DB_CFG['pw'],
                           connect_timeout=5)
    with conn.cursor() as cur:
        cur.execute("""SELECT ts, soc, ebox_w, pcc_w, bat1_w, dc_expected_w
                       FROM pv_decision_log
                       WHERE version='waveshare' AND ts >= NOW() - INTERVAL %s MINUTE
                       ORDER BY ts""", (window_min,))
        rows = cur.fetchall()
        cur.execute("""SELECT MAX(ebox_w) FROM pv_decision_log
                       WHERE version='waveshare' AND ts >= CURDATE()""")
        pmax_today = cur.fetchone()[0]
    conn.close()
    if not rows:
        sys.exit("keine pv_decision_log-Daten im Messfenster — laeuft waveshare_compare.py?")
    return rows, float(pmax_today or 0)


def slope_per_h(rows):
    """SOC-Steigung in %/h per kleinster Quadrate ueber das Fenster."""
    t0 = rows[0][0]
    xs = [(r[0] - t0).total_seconds() / 3600.0 for r in rows]
    ys = [float(r[1]) for r in rows]
    n  = len(xs)
    if n < 3:
        return 0.0
    mx, my = sum(xs) / n, sum(ys) / n
    den = sum((x - mx) ** 2 for x in xs)
    return 0.0 if den == 0 else sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den


# ── Prognose ────────────────────────────────────────────────────────────────
def predict_max(dc, now, soc, target, k, clearness, pmax_w):
    """Minutenweise Integration bis Tagesende (bzw. bis target erreicht ist).
    Rueckgabe (exp_max in %, Uhrzeit dazu, geladene kWh).
    Wird target erreicht, ist die Uhrzeit der Zeitpunkt dafuer; sonst der
    letzte Zeitpunkt mit Ertrag — danach steigt der SOC heute nicht mehr."""
    need = max(0.0, target - soc) * k
    e    = 0.0
    t    = now
    last = now                      # letzter Zeitpunkt mit Ertrag
    end  = now.replace(hour=END_HOUR, minute=0)
    while t < end:
        p = max(0.0, min(pmax_w, clearsky_w(dc, t) * clearness - LOAD_W))
        t += dt.timedelta(minutes=1)
        if p > 0:
            e += p / 60 / 1000
            last = t
        if e >= need:
            return target, t, e
    return min(target, soc + e / k), last, e


def main():
    ap = argparse.ArgumentParser(description='Prognose: hoechster heute erwarteter EBox-SOC')
    ap.add_argument('--window', type=int, default=20, help='Messfenster in Minuten (Default 20)')
    ap.add_argument('--target', type=float, default=100.0, help='Ziel-SOC in %% (Default 100)')
    ap.add_argument('--pmax',   type=float, default=None, help='Ladedeckel in W (Default: Tagesmaximum)')
    ap.add_argument('--json',   action='store_true', help='Ausgabe als JSON')
    args = ap.parse_args()

    dc  = load_dc_forecast()
    now = dt.datetime.now(dc._TZ).replace(second=0, microsecond=0)
    rows, pmax_today = fetch_trend(args.window)

    soc      = float(rows[-1][1])
    stamp    = rows[-1][0]
    ebox_avg = sum(float(r[2] or 0) for r in rows) / len(rows)
    d_soc_h  = slope_per_h(rows)

    # kWh je Prozent aus dem Fenster selbst; nur uebernehmen wenn plausibel.
    k_meas = None
    if d_soc_h > 0.5 and ebox_avg > 200:
        k_meas = (ebox_avg / 1000.0) / d_soc_h
    k_ok = bool(k_meas and K_RANGE[0] <= k_meas <= K_RANGE[1])
    k    = k_meas if k_ok else K_DEFAULT

    # Bewölkung: Ist/Klarhimmel im selben Fenster (Proxy wie die ESP-Ratio).
    ist  = sum(float(r[3] or 0) + float(r[2] or 0) + float(r[4] or 0) for r in rows) / len(rows)
    soll = sum(float(r[5] or 0) for r in rows) / len(rows)
    clearness = max(0.0, min(1.0, ist / soll)) if soll > 1000 else 0.0

    # Ladedeckel: Tagesmaximum; solange heute kaum geladen wurde, taugt das nicht
    # als Deckel → dann Stufe 7 (gleiche Regel wie fox2db_logic.h:eta_soc100_h).
    pmax = args.pmax if args.pmax else (pmax_today if pmax_today > 1000 else PMAX_W)
    need = max(0.0, (args.target - soc)) * k

    res = {'version': VERSION, 'jetzt': now.strftime('%Y-%m-%d %H:%M'),
           'soc': round(soc, 1), 'soc_stand': stamp.strftime('%H:%M'),
           'ziel': args.target, 'rest_kwh': round(need, 1),
           'kwh_pro_prozent': round(k, 3), 'k_gemessen': round(k_meas, 3) if k_meas else None,
           'k_quelle': 'gemessen' if k_ok else 'default',
           'trend_prozent_h': round(d_soc_h, 1), 'ebox_avg_w': round(ebox_avg),
           'pmax_w': round(pmax), 'clearness': round(clearness, 2),
           'fenster_min': args.window}

    if need <= 0:
        res['exp_max'], res['exp_max_h'] = soc, now.strftime('%H:%M')
        res['erreichbar'] = True
        szenarien = []
    else:
        m_now, t_now, e_now = predict_max(dc, now, soc, args.target, k, clearness, pmax)
        res['exp_max']   = round(m_now, 1)
        res['exp_max_h'] = t_now.strftime('%H:%M')
        res['erreichbar'] = m_now >= args.target
        res['rest_kwh'] = round(e_now, 1)
        # Bandbreite: nicht nur das Wetter, auch die tatsaechlich ankommende
        # Ladeleistung variieren — bei klarem Himmel deckelt sonst allein pmax
        # und alle Szenarien fallen auf dieselbe Uhrzeit zusammen.
        szenarien = []
        for cl, pm, lab in ((min(1.0, clearness * 1.2), PMAX_W,       'optimistisch (Stufe 7 voll)'),
                            (clearness,                 pmax,          'Trend haelt'),
                            (clearness * 0.5,           ebox_avg * 0.7, 'Wolken wie am Vormittag')):
            m_s, t_s, _ = predict_max(dc, now, soc, args.target, k, cl, pm)
            szenarien.append({'fall': lab, 'clearness': round(cl, 2),
                              'pmax_w': round(pm), 'exp_max': round(m_s, 1),
                              'exp_max_h': t_s.strftime('%H:%M')})
        res['szenarien'] = szenarien

    if args.json:
        print(json.dumps(res, ensure_ascii=False))
        return

    print(f"EBox-SOC {soc:.1f} % (Stand {stamp:%H:%M}) → Ziel {args.target:.0f} %, "
          f"Restbedarf {need:.1f} kWh")
    print(f"Trend ({args.window} min): {d_soc_h:+.1f} %/h bei {ebox_avg/1000:.1f} kW "
          f"→ {k:.2f} kWh/% ({'gemessen' if k_ok else 'Default'})")
    print(f"Bewölkung: Ist/Klarhimmel = {clearness:.2f}  |  Ladedeckel {pmax/1000:.1f} kW")
    if need <= 0:
        print("Ziel-SOC bereits erreicht.")
        return
    print(f"\n→ erwartetes Maximum {res['exp_max']:.0f} % um {res['exp_max_h']} Uhr"
          + ("" if res['erreichbar'] else "  (Ziel heute nicht erreichbar)"))
    print("\nBandbreite:")
    for s in szenarien:
        print(f"  {s['fall']:27s} clearness {s['clearness']:.2f}, "
              f"{s['pmax_w']/1000:4.1f} kW → {s['exp_max']:5.1f} % um {s['exp_max_h']}")


if __name__ == '__main__':
    main()
