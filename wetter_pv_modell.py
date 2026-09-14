#!/usr/bin/env python3
"""
wetter_pv_modell.py — Wie hängt die PV-Tagesernte vom Wetter desselben Tages ab?

Eingänge (CSV-Exporte, siehe --dir):
  weather.csv          heissa wagodb.weather_data (OWM, 3-h-Raster; Lenggries/GAP/Bobingen)
  inv_pi.csv           kellertreppe wagodb.inverter_data_daily  (device_id=1, Kunde1)
  inv_pi_raw.csv       kellertreppe inverter_data ab 15.06.2026, je Tag verdichtet
  inv_heissa.csv       heissa wagodb.inverter_data_daily         (device_id=2, Bob)
  inv_heissa_raw.csv   heissa inverter_data ab 15.06.2026, je Tag verdichtet

Achtung: weather_data ist die OWM-*Vorhersage*, laufend überschrieben; für
vergangene Zeitpunkte steht dort der letzte Lauf davor (≈ Nowcast), keine Messung.

Modell (je Tag: Wetter des Tages ↔ Ernte des Tages):
  Ernte = Klarhimmel(Tag) · kt
  Klarhimmel = H0(Tag, Breite) · k_Jahr   (H0 astronomisch, k = 98.-Perzentil je Jahr,
                                           fängt Anlagenänderungen zwischen Jahren ab)
  kt = Wetteranteil 0…1, erklärt aus Tageslicht-Mitteln von Wolken, Regen, Temp., …
  (1) nur Wolken, linear       → „je 10 % Wolken kostet x %"
  (2) alle Merkmale, linear
  (3) Gradient Boosting        → Obergrenze der Erklärbarkeit
  Güte per Kreuzvalidierung über 14-Tage-Blöcke.
"""
import argparse
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.model_selection import GroupKFold, cross_val_predict
from sklearn.inspection import permutation_importance
from sklearn.metrics import r2_score, mean_absolute_error

LOC = {47.6833: 'lenggries', 47.4921: 'gap', 48.2711: 'bobingen'}
WX = ['cloud', 'rain', 'pop', 'temp', 'hum', 'vis', 'wind', 'wx_clear', 'wx_rain', 'wx_snow']
FEAT = WX + ['saison_sin', 'saison_cos']


def h0_daily(doy, lat_deg):
    """Extraterrestrische Tagesstrahlung auf die Horizontale [kWh/m²]."""
    phi = np.radians(lat_deg)
    dr = 1 + 0.033 * np.cos(2 * np.pi * doy / 365)
    dec = 0.409 * np.sin(2 * np.pi * doy / 365 - 1.39)
    ws = np.arccos(np.clip(-np.tan(phi) * np.tan(dec), -1, 1))
    mj = 24 * 60 / np.pi * 0.0820 * dr * (ws * np.sin(phi) * np.sin(dec)
                                         + np.cos(phi) * np.cos(dec) * np.sin(ws))
    return mj / 3.6


def weather_daily(path):
    w = pd.read_csv(path, parse_dates=['timestamp'])
    w['loc'] = w.latitude.round(4).map(LOC)
    w = w[w.part_of_day == 'd']                      # nur Tageslicht-Slots
    w['tag'] = w.timestamp.dt.normalize()
    wx = w.weather.str.lower()
    w['wx_clear'] = (wx == 'clear sky').astype(float)
    w['wx_rain'] = wx.str.contains('rain|drizzle|thunder').astype(float)
    w['wx_snow'] = wx.str.contains('snow|sleet').astype(float)
    g = w.groupby(['loc', 'tag']).agg(
        cloud=('cloudiness', 'mean'), rain=('rain_3h', 'sum'), pop=('pop', 'mean'),
        temp=('temperature', 'mean'), hum=('humidity', 'mean'),
        vis=('visibility', 'mean'), wind=('wind_speed', 'mean'),
        wx_clear=('wx_clear', 'mean'), wx_rain=('wx_rain', 'mean'),
        wx_snow=('wx_snow', 'mean'), slots=('cloudiness', 'size'))
    g = g[g.slots >= 2].drop(columns='slots')        # Winter: nur 2 Tageslicht-Slots
    return g.reset_index()


def pv_daily(daily_csv, raw_csv, dev):
    d = pd.read_csv(daily_csv)
    d = d[d.dim.str.startswith(f'device_id={dev}')]
    today = d[d.spalte == 'PV_Generation_Today'].set_index('tag')
    total = d[d.spalte == 'PV_Generation_Total'].set_index('tag')
    a = pd.DataFrame({'kwh': today.maxv, 'n': today.n,
                      'delta': total.maxv - total.minv})
    try:
        r = pd.read_csv(raw_csv)
        r = r[r.device_id.astype(str) == str(dev)].set_index('tag')
        b = pd.DataFrame({'kwh': r.pvgen, 'n': r.n})
        if 'totmax' in r:
            b['delta'] = r.totmax - r.totmin
        a = pd.concat([a, b])
    except FileNotFoundError:
        pass
    # Registerspitzen: Tageszähler über der Differenz des Gesamtzählers → Differenz nehmen
    spike = a.delta.gt(0) & (a.kwh > 1.1 * a.delta)
    a.loc[spike, 'kwh'] = a.delta[spike]
    a.index = pd.to_datetime(a.index)
    a = a[a.index < pd.Timestamp.today().normalize()]  # heutiger Teiltag raus
    a = a[a.n >= 0.25 * a.n.median()]                  # Tage mit Logging-Lücken raus
    a = a[~a.index.duplicated(keep='last')].sort_index()
    return a.kwh, int(spike.sum())


def fit(name, kwh, wd, loc):
    df = wd[wd['loc'] == loc].set_index('tag').join(kwh, how='inner').dropna()
    df = df[df.kwh > 0.2]
    lat = [k for k, v in LOC.items() if v == loc][0]
    doy = df.index.dayofyear.values
    df['h0'] = h0_daily(doy, lat)
    df['saison_sin'] = np.sin(2 * np.pi * doy / 365)
    df['saison_cos'] = np.cos(2 * np.pi * doy / 365)
    # Anlagenfaktor je Jahr: 98. Perzentil von kWh/H0 = "Klarhimmel-Tag", nur Apr–Sep:
    # geneigte Module holen im Winter relativ zur Horizontalen H0 zu viel heraus
    ratio = df.kwh / df.h0
    sommer = ratio[(df.index.month >= 4) & (df.index.month <= 9)]
    kj = sommer.groupby(sommer.index.year).quantile(0.98)
    df['klar'] = df.h0 * df.index.year.map(kj).values
    df = df[df.kwh <= 1.5 * df.klar]                 # restliche Ausreißer raus
    df['kt'] = df.kwh / df.klar

    X, y = df[FEAT].values, df.kt.values
    # Kreuzvalidierung über ganze 14-Tage-Blöcke: benachbarte Tage sind
    # wettermäßig verwandt und dürfen nicht zugleich in Training und Test liegen
    blocks = (df.index - df.index.min()).days // 14
    cv = GroupKFold(n_splits=5)
    wolke = LinearRegression()
    lin = LinearRegression()
    gbr = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05,
                                        max_leaf_nodes=15, min_samples_leaf=10)
    res = {}
    for mname, m, cols in (('nur Wolken', wolke, ['cloud']), ('linear', lin, FEAT),
                           ('boost', gbr, FEAT)):
        p_cv = cross_val_predict(m, df[cols].values, y, cv=cv, groups=blocks)
        kwh_cv = p_cv * df.klar.values
        res[mname] = (r2_score(df.kwh, kwh_cv), mean_absolute_error(df.kwh, kwh_cv))
    base = df.kt.mean() * df.klar                    # Referenz: nur Sonnenstand
    res['nur Saison'] = (r2_score(df.kwh, base), mean_absolute_error(df.kwh, base))

    wolke.fit(df[['cloud']].values, y)
    lin.fit((X - X.mean(0)) / X.std(0), y)           # standardisiert → vergleichbar
    gbr.fit(X, y)
    pi = permutation_importance(gbr, X, y, n_repeats=10, random_state=0)
    return dict(name=name, loc=loc, n=len(df), von=df.index.min().date(),
                bis=df.index.max().date(), kj=kj, res=res, df=df, gbr=gbr,
                wolke=(wolke.intercept_, wolke.coef_[0]),
                coef=dict(zip(FEAT, lin.coef_)), imp=dict(zip(FEAT, pi.importances_mean)),
                r_kwh=df[WX].corrwith(df.kwh), r_kt=df[WX].corrwith(df.kt))


def report(r, spikes):
    df = r['df']
    print(f"\n=== {r['name']} · Wetter {r['loc']} · {r['n']} Tage {r['von']} … {r['bis']}"
          f"  ({spikes} Registerspitzen korrigiert)")
    for j, k in r['kj'].items():
        print(f"  Klarhimmel-Ernte {j}: Juni ≈ {k * h0_daily(172, 47.7):.0f} kWh, "
              f"Dez ≈ {k * h0_daily(355, 47.7):.0f} kWh")
    print(f"  Mittel {df.kwh.mean():.1f} kWh/Tag, Summe {df.kwh.sum():.0f} kWh")
    a, b = r['wolke']
    print(f"  Faustformel: Ernte ≈ Klarhimmel · ({a:.2f} − {-b * 100:.2f} · Wolken%/100)"
          f"  → je 10 % Wolken −{-b * 10 * 100:.1f} % der Klarhimmel-Ernte")
    print("  Güte (Kreuzvalidierung, auf kWh):")
    for m, (r2, mae) in r['res'].items():
        print(f"    {m:<11} R² {r2:5.2f}   MAE {mae:6.1f} kWh")
    print("  Merkmal     r(kWh)  r(kt)  lin.β(std)  Wichtigkeit")
    for f in WX:
        print(f"    {f:<9} {r['r_kwh'][f]:+.2f}   {r['r_kt'][f]:+.2f}   "
              f"{r['coef'][f]:+.3f}      {r['imp'][f]:.3f}")
    b = pd.cut(df.cloud, [-1, 10, 30, 50, 70, 90, 101],
               labels=['0-10', '10-30', '30-50', '50-70', '70-90', '90-100'])
    t = df.groupby(b, observed=True).agg(tage=('kt', 'size'), kt=('kt', 'mean'),
                                         kwh=('kwh', 'mean'))
    print("  Wolken % → Anteil der Klarhimmel-Ernte (kt):")
    print(t.round(2).to_string())
    s = df.groupby([df.index.year, df.index.month]).agg(
        tage=('kt', 'size'), kt=('kt', 'mean'), cloud=('cloud', 'mean'),
        kwh=('kwh', 'mean'), klar=('klar', 'mean'))
    s['wetterverlust'] = s.klar - s.kwh
    print("  Je Monat (Mittel kWh/Tag; Wetterverlust = Klarhimmel − Ist):")
    print(s.round(1).to_string())


def plot(results, out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(len(results), 2, figsize=(14, 4.2 * len(results)), squeeze=False)
    for i, r in enumerate(results):
        df = r['df']
        pred = r['gbr'].predict(df[FEAT].values) * df.klar
        a = ax[i, 0]
        a.fill_between(df.index, 0, df.klar, color='#f2c14e', alpha=.25, label='Klarhimmel')
        a.plot(df.index, df.kwh, '.', ms=4, color='#1f5f8b', label='Ist')
        a.plot(df.index, pred, '-', lw=.8, color='#c0392b', label='Wettermodell')
        a.set_title(f"{r['name']} – Tagesernte kWh (Wetter {r['loc']})")
        a.legend(loc='upper left', fontsize=8)
        a = ax[i, 1]
        sc = a.scatter(df.cloud, df.kt, c=df.index.month, cmap='twilight', s=14)
        x = np.array([0, 100])
        a.plot(x, r['wolke'][0] + r['wolke'][1] * x, 'k--', lw=1, label='nur Wolken, linear')
        a.set_xlabel('Wolken % (Tageslicht)'); a.set_ylabel('kt = Ist / Klarhimmel')
        a.set_title('Wolkenbedeckung vs. Ausbeute (Farbe = Monat)')
        a.legend(fontsize=8)
        fig.colorbar(sc, ax=a, label='Monat')
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    print(f"\nPlot: {out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dir', default='.')
    ap.add_argument('--png', default='wetter_pv_modell.png')
    a = ap.parse_args()
    d = a.dir.rstrip('/') + '/'
    wd = weather_daily(d + 'weather.csv')
    anlagen = [('Kunde1 Lenggries (klein, device_id=1)', 'lenggries',
                d + 'inv_pi.csv', d + 'inv_pi_raw.csv', 1),
               ('Bob Bobingen (groß, device_id=2)', 'bobingen',
                d + 'inv_heissa.csv', d + 'inv_heissa_raw.csv', 2)]
    results = []
    for name, loc, daily, raw, dev in anlagen:
        kwh, spikes = pv_daily(daily, raw, dev)
        cand = {l: fit(name, kwh, wd, l) for l in LOC.values()}
        print(f"\n{name}: Kontrolle R² mit fremdem Wetter " +
              ", ".join(f"{l} {c['res']['boost'][0]:.2f}" for l, c in cand.items()))
        report(cand[loc], spikes)
        results.append(cand[loc])
    plot(results, a.png)


if __name__ == '__main__':
    main()
