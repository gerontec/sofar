#!/usr/bin/env python3
"""Taegliche Lade-/Entladebilanz der EBox aus `wagodb.pv_ebox2`.

Die Tabelle ist der Rohspeicher von `ebox_mqtt.py` auf dem Pi: pro Minute eine
Zeile je Pack, Spalte `Power` ist dabei **nicht** die Leistung, sondern die
Pack-Nummer (1..3). Leistung wird hier gerechnet: `Volt` (mV) x `Curr` (mA).
Vorzeichen kommt vom BMS — positiver Strom laedt, negativer entlaedt.

Sichtbar sind nur 3 Packs; parallel haengt eine zweite 15-kWh-EBox ohne
Datenausgang daran. Deshalb derselbe Faktor 2 wie in `ebox_mqtt.py`
(`EBOX_MULT`); mit `--faktor 1` sieht man nur die messbare Haelfte.

    ./rep_ebox.py                     # letzte 14 Tage
    ./rep_ebox.py -n 30               # letzte 30 Tage
    ./rep_ebox.py --von 2026-08-01 --bis 2026-08-31
    ./rep_ebox.py --stunden 2026-08-30   # ein Tag, Stunde fuer Stunde
    ./rep_ebox.py --monat             # Monatssummen
    ./rep_ebox.py --csv               # zum Weiterrechnen
    ./rep_ebox.py --roh               # an pv_ebox_tag vorbei, direkt aus den Minuten

Die Rohminuten in `pv_ebox2` reichen nur drei Monate zurueck — was aelter ist,
raeumt `ebox_rollup.py --aufraeumen` naechtlich weg und hinterlaesst die
Tageszeile in `pv_ebox_tag`. Tages- und Monatssicht lesen deshalb das Rollup
und rechnen allein den laufenden Tag live aus den Minuten. `--stunden` braucht
die Minuten und funktioniert nur innerhalb des Fensters; `--roh` erzwingt
ueberall die Direktrechnung, zum Gegenpruefen des Rollups.

Gegenprobe eingebaut: `netto` (geladen minus entladen) muss dem SOC-Hub des
Tages entsprechen, also (SOC_Ende - SOC_Start) x Kapazitaet / 100. Die Spalte
`Diff` zeigt die Abweichung; sie ist normal leicht positiv (Ladeverluste, rund
0,5 kWh am Tag). Ein negatives `netto` ist damit kein Widerspruch zum
10-%-Abschaltpunkt, sondern schlicht ein Tag, an dem der SOC gefallen ist.

Energie wird als Minutenintegral gebildet: mittlere Leistung der Minute / 60.
Fehlt eine Minute (Cron ausgefallen), fehlt sie auch in der Summe — deshalb
steht die Minutenabdeckung mit in der Tabelle, 1440 ist ein voller Tag.
"""
import argparse
import csv
import os
import sys
from datetime import date, datetime, timedelta

import pymysql

DB_HOST = os.environ.get("EBOX_DB_HOST", "192.168.178.218")
DB = dict(user="gh", password="a12345", database="wagodb",
          charset="utf8mb4", connect_timeout=5,
          cursorclass=pymysql.cursors.DictCursor)

# Zwei parallele 15-kWh-Boxen, nur eine meldet sich (siehe ebox_mqtt.EBOX_MULT).
FAKTOR_VORGABE = 2.0
KAPAZITAET_VORGABE = 30.0   # kWh — 2 x 15 kWh, beide Boxen zusammen

# Minutenmittel der Gesamtleistung in W. Gemittelt wird ueber die Packs einer
# Minute und dann auf 3 Packs hochgerechnet: faellt ein Pack in einer Minute
# aus, kippt das Ergebnis dadurch nicht auf zwei Drittel.
MINUTE_CTE = """
WITH m AS (
    SELECT ts_min,
           AVG(Volt * Curr / 1000000.0) * 3 * %(faktor)s AS p,
           AVG(Coulomb) AS soc
    FROM pv_ebox2
    WHERE ts >= %(von)s AND ts < %(bis)s
      AND Power IN (1, 2, 3)
      -- eine Zeile (30.01.2026) hat verrutschte Felder: Volt 5292, Coulomb 52983
      AND Volt BETWEEN 30000 AND 60000
      AND ABS(Curr) <= 60000
      AND Coulomb BETWEEN 0 AND 100
    GROUP BY ts_min
)
"""


def _verbinde(host):
    return pymysql.connect(host=host, **DB)


# Fertige Tageszeilen aus dem Rollup. Dieselben Felder wie die Direktrechnung,
# damit Ausgabe und CSV nicht zwischen den Quellen unterscheiden muessen.
ROLLUP_SQL = """
SELECT {k}                                  AS bucket,
       SUM(minuten)                         AS minuten,
       SUM(lad_kwh)                         AS lade_kwh,
       SUM(entl_kwh)                        AS entlade_kwh,
       MAX(lad_max_w)                       AS p_lade_max,
       MIN(entl_max_w)                      AS p_entlade_max,
       MIN(soc_min)                         AS soc_min,
       MAX(soc_max)                         AS soc_max,
       SUBSTRING_INDEX(GROUP_CONCAT(soc_start ORDER BY d ASC),  ',', 1) AS soc_start,
       SUBSTRING_INDEX(GROUP_CONCAT(soc_ende  ORDER BY d DESC), ',', 1) AS soc_ende
FROM pv_ebox_tag
WHERE d >= %(von)s AND d < %(bis)s
GROUP BY bucket
ORDER BY bucket
"""


def hole_rollup(host, von, bis, gruppe):
    """Tages- oder Monatszeilen aus pv_ebox_tag, plus den laufenden Tag live.

    Das Rollup enthaelt absichtlich nur abgeschlossene Tage; heute wird
    nachgerechnet, sonst fehlte der angebrochene Tag in jeder Uebersicht.
    """
    schluessel = {"tag": "d", "monat": "DATE_FORMAT(d, '%%Y-%%m-01')"}[gruppe]
    with _verbinde(host) as conn:
        with conn.cursor() as cur:
            cur.execute(ROLLUP_SQL.format(k=schluessel), dict(von=von, bis=bis))
            zeilen = cur.fetchall()
    heute = date.today()
    if von <= heute < bis:
        live = hole_minuten(host, heute, heute + timedelta(days=1),
                            FAKTOR_VORGABE, "tag")
        for z in live:
            if gruppe == "monat":
                # Der laufende Monat steht schon in der Liste, sofern er
                # abgeschlossene Tage hat — dann dazuzaehlen statt anhaengen.
                # DATE_FORMAT liefert einen String — der Live-Bucket muss
                # dieselbe Form haben, sonst findet er seine Monatszeile nicht.
                z = dict(z, bucket=heute.strftime("%Y-%m-01"))
                for alt in zeilen:
                    if alt["bucket"] == z["bucket"]:
                        _addiere(alt, z)
                        break
                else:
                    zeilen.append(z)
            else:
                zeilen.append(z)
    return zeilen


def _addiere(alt, neu):
    """Laufenden Tag in die Monatszeile einrechnen."""
    m_alt, m_neu = float(alt["minuten"] or 0), float(neu["minuten"] or 0)
    alt["lade_kwh"] = float(alt["lade_kwh"]) + float(neu["lade_kwh"])
    alt["entlade_kwh"] = float(alt["entlade_kwh"]) + float(neu["entlade_kwh"])
    alt["minuten"] = int(m_alt + m_neu)
    alt["p_lade_max"] = max(float(alt["p_lade_max"] or 0), float(neu["p_lade_max"] or 0))
    alt["p_entlade_max"] = min(float(alt["p_entlade_max"] or 0), float(neu["p_entlade_max"] or 0))
    alt["soc_min"] = min(float(alt["soc_min"]), float(neu["soc_min"]))
    alt["soc_max"] = max(float(alt["soc_max"]), float(neu["soc_max"]))
    alt["soc_ende"] = neu["soc_ende"]


def hole_tage(host, von, bis, faktor, gruppe="tag", roh=False):
    """Tages-/Monatszeilen — aus dem Rollup, oder auf Wunsch direkt aus den Minuten."""
    if not roh and gruppe in ("tag", "monat"):
        try:
            return hole_rollup(host, von, bis, gruppe)
        except pymysql.Error:
            pass   # kein Rollup vorhanden -> Direktrechnung
    return hole_minuten(host, von, bis, faktor, gruppe)


def hole_minuten(host, von, bis, faktor, gruppe="tag"):
    """Eine Zeile je Tag (oder Monat) mit Energie, Spitzen und SOC-Verlauf."""
    schluessel = {"tag": "DATE(ts_min)",
                  "monat": "DATE_FORMAT(ts_min, '%%Y-%%m-01')",
                  "stunde": "DATE_FORMAT(ts_min, '%%Y-%%m-%%d %%H:00:00')"}[gruppe]
    sql = MINUTE_CTE + """
    , g AS (
        SELECT {k} AS bucket, ts_min, p, soc,
               ROW_NUMBER() OVER (PARTITION BY {k} ORDER BY ts_min ASC)  AS rn_a,
               ROW_NUMBER() OVER (PARTITION BY {k} ORDER BY ts_min DESC) AS rn_e
        FROM m
    )
    SELECT bucket,
           COUNT(*)                                  AS minuten,
           SUM(GREATEST(p, 0))  / 60000.0            AS lade_kwh,
           SUM(GREATEST(-p, 0)) / 60000.0            AS entlade_kwh,
           GREATEST(MAX(p), 0)                       AS p_lade_max,
           LEAST(MIN(p), 0)                          AS p_entlade_max,
           MIN(soc)                                  AS soc_min,
           MAX(soc)                                  AS soc_max,
           MAX(CASE WHEN rn_a = 1 THEN soc END)      AS soc_start,
           MAX(CASE WHEN rn_e = 1 THEN soc END)      AS soc_ende
    FROM g
    GROUP BY bucket
    ORDER BY bucket
    """.format(k=schluessel)
    with _verbinde(host) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, dict(von=von, bis=bis, faktor=faktor))
            return cur.fetchall()


def _f(wert, stellen=2):
    return 0.0 if wert is None else round(float(wert), stellen)


def _dsoc(zeile, kapazitaet):
    """Der SOC-Hub der Periode in kWh — die unabhaengige Gegenprobe zur Bilanz."""
    a, e = zeile["soc_start"], zeile["soc_ende"]
    if a is None or e is None:
        return 0.0
    return (float(e) - float(a)) * kapazitaet / 100.0


def _name(bucket, gruppe, lang=False):
    """Beschriftung der Zeile — die SQL-Buckets kommen je nach Gruppe als
    date (Tag) oder als String aus DATE_FORMAT (Stunde, Monat)."""
    if gruppe == "monat":
        return str(bucket)[:7]
    if gruppe == "stunde":
        return str(bucket)[8:10] + "." + str(bucket)[5:7] + ". " + str(bucket)[11:16]
    return f"{bucket:%a %d.%m.%Y}" if lang else f"{bucket:%a %d.%m.}"


def balken(lade, entlade, breite=18):
    """Zwei Halbbalken: links entladen, rechts geladen, gleiche Skala."""
    spanne = max(lade, entlade, 0.001)
    l = int(round(entlade / spanne * breite))
    r = int(round(lade / spanne * breite))
    return " " * (breite - l) + "#" * l + "|" + "=" * r + " " * (breite - r)


def drucke_tabelle(zeilen, gruppe, kapazitaet):
    kopf = {"tag": "Tag", "monat": "Monat", "stunde": "Stunde"}[gruppe]
    print(f"{kopf:<19} {'geladen':>9} {'entladen':>9} {'netto':>8} "
          f"{'Lade-Spitze':>12} {'Entl.-Spitze':>12} "
          f"{'SOC Start>Ende':>15} {'min/max':>8} {'dSOC':>7} {'Diff':>6} {'Min':>6}")
    print(f"{'':<19} {'kWh':>9} {'kWh':>9} {'kWh':>8} "
          f"{'':>12} {'':>12} {'':>15} {'':>8} {'kWh':>7} {'kWh':>6}")
    print("-" * 121)
    s_lade = s_entlade = 0.0
    for z in zeilen:
        lade, entlade = _f(z["lade_kwh"]), _f(z["entlade_kwh"])
        s_lade += lade
        s_entlade += entlade
        dsoc = _dsoc(z, kapazitaet)
        name = _name(z["bucket"], gruppe, lang=True)
        print(f"{name:<19} {lade:>9.2f} {entlade:>9.2f} {lade - entlade:>+8.2f} "
              f"{_f(z['p_lade_max'], 0):>11.0f}W {_f(z['p_entlade_max'], 0):>11.0f}W "
              f"{_f(z['soc_start'], 0):>4.0f}->{_f(z['soc_ende'], 0):<4.0f}"
              f"{_f(z['soc_min'], 0):>4.0f}-{_f(z['soc_max'], 0):<4.0f}"
              f"{dsoc:>+7.2f} {lade - entlade - dsoc:>+6.2f}"
              f"{z['minuten']:>6}")
    if not zeilen:
        print("(keine Daten im Zeitraum)")
        if gruppe == "stunde":
            print("Die Stundensicht braucht die Rohminuten — die reichen nur drei "
                  "Monate zurueck.\nAeltere Tage gibt es nur noch als Tageszeile "
                  "aus pv_ebox_tag.")
        return
    print("-" * 121)
    tage = len(zeilen)
    hub = _dsoc({"soc_start": zeilen[0]["soc_start"],
                 "soc_ende": zeilen[-1]["soc_ende"]}, kapazitaet)
    print(f"{'Summe':<19} {s_lade:>9.2f} {s_entlade:>9.2f} {s_lade - s_entlade:>+8.2f} "
          f"{'':>12} {'':>12} {'':>15} {'':>8}{hub:>+7.2f} "
          f"{s_lade - s_entlade - hub:>+5.2f}")
    print(f"{'Mittel je Zeile':<19} {s_lade / tage:>9.2f} {s_entlade / tage:>9.2f}")
    if kapazitaet > 0:
        print(f"{'Vollzyklen':<19} {s_entlade / kapazitaet:>9.2f}  "
              f"(entladen / {kapazitaet:.0f} kWh, {s_entlade / kapazitaet / tage:.2f} je Zeile)")
    print()
    print(f"{'':<19} {'entladen':>18} | {'geladen':<18}   (Skala je Zeile bis "
          f"{max(max(_f(z['lade_kwh']), _f(z['entlade_kwh'])) for z in zeilen):.1f} kWh)")
    for z in zeilen:
        print(f"{_name(z['bucket'], gruppe):<19} "
              f"{balken(_f(z['lade_kwh']), _f(z['entlade_kwh']))}")


def drucke_csv(zeilen, gruppe, kapazitaet):
    schreiber = csv.writer(sys.stdout)
    schreiber.writerow(["bucket", "minuten", "lade_kwh", "entlade_kwh", "netto_kwh",
                        "dsoc_kwh", "diff_kwh",
                        "p_lade_max_w", "p_entlade_max_w",
                        "soc_start", "soc_ende", "soc_min", "soc_max"])
    for z in zeilen:
        lade, entlade = _f(z["lade_kwh"]), _f(z["entlade_kwh"])
        dsoc = _dsoc(z, kapazitaet)
        schreiber.writerow([z["bucket"], z["minuten"], f"{lade:.3f}", f"{entlade:.3f}",
                            f"{lade - entlade:.3f}",
                            f"{dsoc:.3f}", f"{lade - entlade - dsoc:.3f}",
                            f"{_f(z['p_lade_max'], 0):.0f}", f"{_f(z['p_entlade_max'], 0):.0f}",
                            f"{_f(z['soc_start'], 1):.1f}", f"{_f(z['soc_ende'], 1):.1f}",
                            f"{_f(z['soc_min'], 1):.1f}", f"{_f(z['soc_max'], 1):.1f}"])


def main():
    p = argparse.ArgumentParser(
        description="Taegliche Lade-/Entladebilanz der EBox aus pv_ebox2.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Beispiel: ./rep_ebox.py --stunden 2026-08-30")
    p.add_argument("-n", "--tage", type=int, default=14, help="Anzahl Tage rueckwaerts (Vorgabe 14)")
    p.add_argument("--von", help="Startdatum JJJJ-MM-TT (einschliesslich)")
    p.add_argument("--bis", help="Enddatum JJJJ-MM-TT (einschliesslich)")
    p.add_argument("--stunden", metavar="DATUM", help="ein Tag, aufgeteilt in Stunden")
    p.add_argument("--monat", action="store_true", help="statt Tagen nach Monaten summieren")
    p.add_argument("--faktor", type=float, default=FAKTOR_VORGABE,
                   help="Hochrechnung auf die zweite, datenlose EBox (Vorgabe 2.0)")
    p.add_argument("--kapazitaet", type=float, default=KAPAZITAET_VORGABE,
                   help="nutzbare Kapazitaet in kWh fuer die Zyklenzahl (Vorgabe 30)")
    p.add_argument("--roh", action="store_true",
                   help="an pv_ebox_tag vorbei direkt aus den Minuten rechnen")
    p.add_argument("--host", default=DB_HOST, help=f"MariaDB-Host (Vorgabe {DB_HOST})")
    p.add_argument("--csv", action="store_true", help="CSV statt Tabelle")
    a = p.parse_args()

    if a.stunden:
        tag = datetime.strptime(a.stunden, "%Y-%m-%d").date()
        von, bis, gruppe = tag, tag + timedelta(days=1), "stunde"
    else:
        gruppe = "monat" if a.monat else "tag"
        bis = (datetime.strptime(a.bis, "%Y-%m-%d").date() if a.bis else date.today()) + timedelta(days=1)
        von = (datetime.strptime(a.von, "%Y-%m-%d").date() if a.von
               else bis - timedelta(days=a.tage))

    try:
        zeilen = hole_tage(a.host, von, bis, a.faktor, gruppe, a.roh)
    except pymysql.Error as e:
        sys.exit(f"Datenbank {a.host}: {e}")

    if a.csv:
        drucke_csv(zeilen, gruppe, a.kapazitaet)
        return
    hinweis = ("die zweite 15-kWh-Box haengt parallel ohne Datenausgang und wird "
               f"mit Faktor {a.faktor:g} hochgerechnet" if a.faktor != 1
               else "Faktor 1 — nur die 3 messenden Packs")
    print(f"EBox {von} bis {bis - timedelta(days=1)}   Bank {a.kapazitaet:g} kWh "
          f"aus {int(3 * a.faktor)} Packs ({hinweis})")
    quelle = ("pv_ebox2 (Minutenintegral von Volt x Curr)" if a.roh or gruppe == "stunde"
              else "pv_ebox_tag (Tagesrollup) + laufender Tag live aus pv_ebox2")
    print(f"Quelle: {quelle} auf {a.host}")
    print()
    drucke_tabelle(zeilen, gruppe, a.kapazitaet)


if __name__ == "__main__":
    main()
