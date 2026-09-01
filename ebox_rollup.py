#!/usr/bin/env python3
"""Materialisiert die EBox-Tagesbilanz aus `pv_ebox2` nach `pv_ebox_tag`.

Der Minutenintegral-Lauf ueber die Rohtabelle kostet auf dem Pi rund zwei
Sekunden und wird mit jedem Monat teurer — fuer eine Webseite, die ihn bei
jedem Aufruf braucht, ist das zu viel. Abgeschlossene Tage aendern sich nicht
mehr, also werden sie einmal gerechnet und hier abgelegt; `inverter.php` liest
nur noch diese Tabelle und rechnet allein den laufenden Tag live.

    ./ebox_rollup.py                     # fehlende Tage + die letzten 2 Tage neu
    ./ebox_rollup.py --alle              # komplette Historie neu rechnen
    ./ebox_rollup.py --tage 7            # zusaetzlich die letzten 7 Tage neu
    ./ebox_rollup.py --aufraeumen        # danach Rohminuten aelter als 3 Monate loeschen
    ./ebox_rollup.py --aufraeumen --trocken   # nur zeigen, was wegfiele

Mit `--aufraeumen` wird die Rohtabelle auf ein gleitendes Fenster begrenzt:
was aelter als `--monate` (Vorgabe 3) ist, steckt vollstaendig im Tagesrollup
und wird in `pv_ebox2` geloescht. Geloescht wird erst, wenn fuer JEDEN
betroffenen Tag eine Rollup-Zeile existiert — fehlt auch nur eine, bricht der
Lauf ab und ruehrt nichts an. Das ist der einzige unumkehrbare Schritt hier:
die Minutenaufloesung dieser Tage ist danach weg, es bleibt die Tageszeile mit
Energie, SOC-Verlauf und den Spitzen — jede mit dem Zeitstempel, zu dem sie
auftrat. Bei mehreren gleichen Werten (der SOC steht ja in ganzen Prozent)
zaehlt der erste Zeitpunkt des Tages.

Wer die Rohminuten noch liest, braucht sie nur frisch: fox2db (letzte 3 min),
r290_boost (aktueller SOC), ebox2_monitor (letzte 12 Zeilen), pvprognos
(gestern + heute). Keiner davon greift hinter das Fenster zurueck.

Gerechnet wird identisch zu rep_ebox.py: Minutenmittel der Packleistung
(Volt x Curr), auf 3 Packs hochgerechnet und mit dem Faktor fuer die zweite,
datenlose 15-kWh-Box multipliziert. Der heutige Tag wird bewusst nicht
geschrieben — er waere beim naechsten Lauf ohnehin unvollstaendig.
"""
import argparse
import os
import sys
import time

import pymysql

DB_HOST = os.environ.get("EBOX_DB_HOST", "192.168.178.218")
DB = dict(user="gh", password="a12345", database="wagodb",
          charset="utf8mb4", connect_timeout=10)

FAKTOR = 2.0        # zweite, datenlose EBox — wie EBOX_MULT in ebox_mqtt.py

TABELLE = """
CREATE TABLE IF NOT EXISTS pv_ebox_tag (
    d          DATE        NOT NULL PRIMARY KEY,
    lad_kwh    DECIMAL(8,3) NOT NULL,
    entl_kwh   DECIMAL(8,3) NOT NULL,
    soc_avg    DECIMAL(5,1),
    soc_min    DECIMAL(5,1),
    soc_max    DECIMAL(5,1),
    soc_start  DECIMAL(5,1),
    soc_ende   DECIMAL(5,1),
    lad_max_w    INT,
    lad_max_zeit  TIME,
    entl_max_w   INT,
    entl_max_zeit TIME,
    soc_min_zeit  TIME,
    soc_max_zeit  TIME,
    minuten    SMALLINT UNSIGNED NOT NULL,
    faktor     DECIMAL(3,1) NOT NULL,
    berechnet  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB
"""

# Eine Zeile (30.01.2026) hat verrutschte Felder: Volt 5292, Coulomb 52983.
FILTER = """Power IN (1,2,3)
            AND Volt BETWEEN 30000 AND 60000
            AND ABS(Curr) <= 60000
            AND Coulomb BETWEEN 0 AND 100"""

BERECHNE = """
INSERT INTO pv_ebox_tag
    (d, lad_kwh, entl_kwh, soc_avg, soc_min, soc_max, soc_start, soc_ende,
     lad_max_w, lad_max_zeit, entl_max_w, entl_max_zeit,
     soc_min_zeit, soc_max_zeit, minuten, faktor)
SELECT bucket,
       SUM(GREATEST(p, 0))  / 60000,
       SUM(GREATEST(-p, 0)) / 60000,
       AVG(soc), MIN(soc), MAX(soc),
       MAX(CASE WHEN rn_a = 1 THEN soc END),
       MAX(CASE WHEN rn_e = 1 THEN soc END),
       -- Spitzen samt Uhrzeit, solange die Minuten noch da sind: nach dem
       -- Aufraeumen lassen sie sich nicht mehr nachrechnen. Wurde an dem Tag
       -- gar nicht geladen, bleibt die Uhrzeit leer statt auf die am
       -- wenigsten negative Minute zu zeigen.
       ROUND(GREATEST(MAX(p), 0)),
       CASE WHEN MAX(p) > 0 THEN MAX(CASE WHEN rn_pmax = 1 THEN TIME(ts_min) END) END,
       ROUND(LEAST(MIN(p), 0)),
       CASE WHEN MIN(p) < 0 THEN MAX(CASE WHEN rn_pmin = 1 THEN TIME(ts_min) END) END,
       MAX(CASE WHEN rn_smin = 1 THEN TIME(ts_min) END),
       MAX(CASE WHEN rn_smax = 1 THEN TIME(ts_min) END),
       COUNT(*), %(faktor)s
FROM (
    SELECT DATE(ts_min) AS bucket, ts_min, p, soc,
           ROW_NUMBER() OVER (PARTITION BY DATE(ts_min) ORDER BY ts_min ASC)  AS rn_a,
           ROW_NUMBER() OVER (PARTITION BY DATE(ts_min) ORDER BY ts_min DESC) AS rn_e,
           ROW_NUMBER() OVER (PARTITION BY DATE(ts_min) ORDER BY p DESC, ts_min)   AS rn_pmax,
           ROW_NUMBER() OVER (PARTITION BY DATE(ts_min) ORDER BY p ASC,  ts_min)   AS rn_pmin,
           ROW_NUMBER() OVER (PARTITION BY DATE(ts_min) ORDER BY soc ASC, ts_min)  AS rn_smin,
           ROW_NUMBER() OVER (PARTITION BY DATE(ts_min) ORDER BY soc DESC, ts_min) AS rn_smax
    FROM (
        SELECT ts_min,
               AVG(Volt * Curr / 1000000.0) * 3 * %(faktor)s AS p,
               AVG(Coulomb)                                 AS soc
        FROM pv_ebox2
        WHERE {filter} {zeitraum}
        GROUP BY ts_min
    ) m
) g
WHERE bucket < CURDATE()
GROUP BY bucket
ON DUPLICATE KEY UPDATE
    lad_kwh   = VALUES(lad_kwh),   entl_kwh  = VALUES(entl_kwh),
    soc_avg   = VALUES(soc_avg),   soc_min   = VALUES(soc_min),
    soc_max   = VALUES(soc_max),   soc_start = VALUES(soc_start),
    soc_ende  = VALUES(soc_ende),  minuten   = VALUES(minuten),
    lad_max_w     = VALUES(lad_max_w),     lad_max_zeit  = VALUES(lad_max_zeit),
    entl_max_w    = VALUES(entl_max_w),    entl_max_zeit = VALUES(entl_max_zeit),
    soc_min_zeit  = VALUES(soc_min_zeit),  soc_max_zeit  = VALUES(soc_max_zeit),
    faktor        = VALUES(faktor)
"""


def aufraeumen(conn, monate, chunk, trocken, quiet):
    """Rohminuten jenseits des Fensters loeschen — aber nur abgesicherte Tage.

    Vor dem ersten DELETE wird geprueft, ob wirklich jeder betroffene Tag im
    Rollup steht. Fehlt einer, passiert gar nichts: lieber ein voller
    Rohspeicher als eine Luecke, die niemand mehr fuellen kann.

    Geloescht wird in Haeppchen. Ein DELETE ueber 200.000 Zeilen am Stueck
    haelt auf dem Pi die Schreiber auf, die jede Minute neue Packzeilen
    einliefern; 5.000er-Bloecke mit Atempause dazwischen stoeren niemanden.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT CURDATE() - INTERVAL %s MONTH", (monate,))
        grenze = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*), MIN(ts), MAX(ts) FROM pv_ebox2 WHERE ts < %s", (grenze,))
        anzahl, von, bis = cur.fetchone()
        if not anzahl:
            if not quiet:
                print(f"Aufraeumen: nichts aelter als {grenze}")
            return

        cur.execute("""
            SELECT COUNT(DISTINCT DATE(ts)) FROM pv_ebox2
            WHERE ts < %s AND DATE(ts) NOT IN (SELECT d FROM pv_ebox_tag)
        """, (grenze,))
        fehlend = cur.fetchone()[0]
        if fehlend:
            sys.exit(f"Abbruch: {fehlend} Tag(e) vor {grenze} fehlen in pv_ebox_tag — "
                     f"erst './ebox_rollup.py --alle' laufen lassen.")

        print(f"Aufraeumen: {anzahl} Rohzeilen vor {grenze} "
              f"({von} bis {bis}), alle Tage sind im Rollup gesichert")
        if trocken:
            print("--trocken: nichts geloescht")
            return

        weg = 0
        while True:
            cur.execute("DELETE FROM pv_ebox2 WHERE ts < %s ORDER BY ts LIMIT %s",
                        (grenze, chunk))
            n = cur.rowcount
            conn.commit()
            if not n:
                break
            weg += n
            time.sleep(0.2)
        if not quiet:
            print(f"Aufraeumen: {weg} Rohzeilen geloescht, Fenster jetzt ab {grenze}")


def main():
    p = argparse.ArgumentParser(description="EBox-Tagesbilanz nach pv_ebox_tag materialisieren.")
    p.add_argument("--alle", action="store_true", help="komplette Historie neu rechnen")
    p.add_argument("--tage", type=int, default=2,
                   help="wie viele zurueckliegende Tage neu gerechnet werden (Vorgabe 2)")
    p.add_argument("--aufraeumen", action="store_true",
                   help="nach dem Rollup Rohminuten jenseits des Fensters loeschen")
    p.add_argument("--monate", type=int, default=3,
                   help="Aufbewahrung der Rohminuten in Monaten (Vorgabe 3)")
    p.add_argument("--chunk", type=int, default=5000,
                   help="Zeilen je DELETE-Haeppchen (Vorgabe 5000)")
    p.add_argument("--trocken", action="store_true",
                   help="beim Aufraeumen nur zeigen, was wegfiele")
    p.add_argument("--host", default=DB_HOST)
    p.add_argument("--quiet", action="store_true", help="nur bei Fehlern etwas sagen")
    a = p.parse_args()

    conn = pymysql.connect(host=a.host, **DB)
    try:
        with conn.cursor() as cur:
            cur.execute(TABELLE)

            if a.alle:
                zeitraum, args = "", dict(faktor=FAKTOR)
            else:
                # Alles ab dem juengsten fehlenden Tag, mindestens die letzten
                # --tage: ein nachgetragener Rohwert soll den Tag korrigieren.
                cur.execute("""
                    SELECT LEAST(
                        COALESCE((SELECT DATE_ADD(MAX(d), INTERVAL 1 DAY) FROM pv_ebox_tag),
                                 (SELECT DATE(MIN(ts)) FROM pv_ebox2)),
                        CURDATE() - INTERVAL %s DAY)
                """, (a.tage,))
                ab = cur.fetchone()[0]
                if ab is None:
                    print("pv_ebox2 ist leer — nichts zu tun")
                    return
                zeitraum = "AND ts >= %(ab)s"
                args = dict(faktor=FAKTOR, ab=ab)

            cur.execute(BERECHNE.format(filter=FILTER, zeitraum=zeitraum), args)
            geschrieben = cur.rowcount
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), MIN(d), MAX(d) FROM pv_ebox_tag")
            n, von, bis = cur.fetchone()
        if not a.quiet:
            print(f"pv_ebox_tag: {geschrieben} Zeilen geschrieben, "
                  f"Bestand {n} Tage ({von} bis {bis})")
        if a.aufraeumen:
            aufraeumen(conn, a.monate, a.chunk, a.trocken, a.quiet)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
