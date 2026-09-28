#!/usr/bin/env python3
"""rep_gateway.py – alle Gateways, die unsere LoRaWAN-Geraete je gehoert haben.

Liest die TTN-Uplinks aus `wagodb.loradevice` (dell 192.168.5.23, nur lesend;
Zugangsdaten aus ~/.my.cnf, Gruppe [wagodb])
und zeigt je Gateway: Empfaenge, ersten/letzten Empfang, besten Pegel und den
ungefaehren Standort samt Ortsnamen.

Standort, in dieser Reihenfolge:
  reg   Position, die TTS in rx_metadata mitliefert (Registry oder GPS des
        Gateways) bzw. die eigene Liste GATEWAYS unten
  ~     geschaetzt: Mittel der Geraetepositionen bei Empfang, gewichtet mit der
        Empfangsleistung (10^(RSSI/10)) -- der staerkste Empfang zieht am
        meisten. Nur Rahmen mit frischem Fix zaehlen (TrackerD fPort 2, Pico
        fPort 1, LA66 fPort 20); nachgelieferte Datalog-Saetze (fPort 4) nicht,
        denn deren Empfangsort ist nicht der Fixort. Wenige Empfaenge an der
        Reichweitengrenze liefern nur die Richtung, nicht den Standort: bei
        '~' mit 1-2 Fixes kann das Gateway viele km entfernt stehen.
  ?     kein Standort und kein Fix bei Empfang

Ueber Packet Broker gehoerte Gateways (fremde Netze, TTN v2) erscheinen unter
der Kennung des weiterleitenden Gateways, nicht als 'packetbroker'.

Ortsnamen kommen von Nominatim (OSM) und werden in
~/.cache/rep_gateway_orte.json zwischengespeichert; hoechstens 1 Anfrage/s.

Aufruf: ./rep_gateway.py [--tage N] [--geraet NAME] [--ohne-ort]
"""
import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from math import asin, cos, radians, sin, sqrt

import pymysql

# Zugangsdaten stehen nicht im Repo, sondern in ~/.my.cnf (chmod 600), Gruppe [wagodb]:
#   [wagodb]
#   host=192.168.5.23
#   user=gh
#   password=...
#   database=wagodb
DB = dict(read_default_file=os.path.expanduser("~/.my.cnf"), read_default_group="wagodb",
          charset="utf8mb4", connect_timeout=5)

# Gateways, deren Standort TTS nicht mitliefert, wir aber kennen.
GATEWAYS = {
    "a84041ffff27e318": ("DLOS8N Lenggries", 47.679, 11.579),
    "b827ebfffe3cec15": ("Gymnasium Lenggries", 47.672236, 11.58718),
}

# (Geraet, fPort) -> Rahmen enthaelt einen frischen Fix
FIX_PORTS = {("trackerd-lenggries", 2), ("pico-0e22", 1), ("la66-notfall", 20)}

CACHE = os.path.expanduser("~/.cache/rep_gateway_orte.json")
NOMINATIM = "https://nominatim.openstreetmap.org/reverse"


def km(la1, lo1, la2, lo2):
    p1, p2 = radians(la1), radians(la2)
    a = sin((p2 - p1) / 2) ** 2 + cos(p1) * cos(p2) * sin(radians(lo2 - lo1) / 2) ** 2
    return 12742 * asin(sqrt(a))


def fix_aus(dp):
    """(lat, lon) aus decoded_payload, None ohne brauchbaren Fix."""
    la = dp.get("Latitude", dp.get("latitude"))
    lo = dp.get("Longitude", dp.get("longitude"))
    if isinstance(la, (int, float)) and isinstance(lo, (int, float)) and (la or lo):
        return la, lo
    return None


def gw_kennung(g):
    """(schluessel, anzeigename) je rx_metadata-Eintrag."""
    ids = g.get("gateway_ids") or {}
    pb = g.get("packet_broker")
    if pb:
        fid = pb.get("forwarder_gateway_id") or pb.get("forwarder_gateway_eui") or "?"
        netz = pb.get("forwarder_tenant_id") or pb.get("forwarder_net_id") or "PB"
        return f"pb:{fid}".lower(), f"{fid} (PB {netz})"
    eui = (ids.get("eui") or "").lower()
    gid = ids.get("gateway_id") or eui
    return eui or gid, gid


class Orte:
    def __init__(self, aus):
        self.aus = aus
        self.letzt = 0.0
        try:
            with open(CACHE) as f:
                self.cache = json.load(f)
        except (OSError, ValueError):
            self.cache = {}

    def name(self, la, lo):
        if self.aus:
            return ""
        k = f"{la:.3f},{lo:.3f}"
        if k in self.cache:
            return self.cache[k]
        time.sleep(max(0.0, 1.05 - (time.time() - self.letzt)))
        self.letzt = time.time()
        url = NOMINATIM + "?" + urllib.parse.urlencode(
            dict(lat=la, lon=lo, format="json", zoom=14, **{"accept-language": "de"}))
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "rep_gateway.py (heissa.de)"})
            with urllib.request.urlopen(req, timeout=10) as r:
                a = json.load(r).get("address") or {}
        except Exception:
            return "(Ortsname nicht abrufbar)"
        ort = (a.get("village") or a.get("town") or a.get("city") or a.get("municipality")
               or a.get("hamlet") or "")
        teil = a.get("suburb") or a.get("hamlet") or a.get("isolated_dwelling") or ""
        kreis = a.get("county") or a.get("state") or ""
        txt = ", ".join(x for x in (ort if not teil or teil == ort else f"{ort}-{teil}",
                                    kreis, a.get("country_code", "").upper()) if x)
        self.cache[k] = txt
        try:
            os.makedirs(os.path.dirname(CACHE), exist_ok=True)
            with open(CACHE, "w") as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=0)
        except OSError:
            pass
        return txt


def main():
    ap = argparse.ArgumentParser(description="Gateways mit ungefaehrem Standort")
    ap.add_argument("--tage", type=int, help="nur die letzten N Tage (Vorgabe: alles)")
    ap.add_argument("--geraet", help="nur dieses Geraet (dev_name)")
    ap.add_argument("--ohne-ort", action="store_true", help="keine Ortsnamen abfragen")
    a = ap.parse_args()

    sql = "SELECT ts, dev_name, f_port, raw FROM loradevice WHERE source='TTN' AND event='up'"
    arg = []
    if a.tage:
        sql += " AND ts >= %s"
        arg.append(datetime.now() - timedelta(days=a.tage))
    if a.geraet:
        sql += " AND dev_name = %s"
        arg.append(a.geraet)
    sql += " ORDER BY ts"
    try:
        conn = pymysql.connect(**DB)
    except pymysql.MySQLError as e:
        sys.exit(f"wagodb nicht erreichbar: {e}")
    with conn.cursor() as cur:
        cur.execute(sql, arg)
        zeilen = cur.fetchall()
    conn.close()

    gw = {}
    for ts, dev, port, raw in zeilen:
        try:
            up = json.loads(raw).get("uplink_message") or {}
        except ValueError:
            continue
        fix = fix_aus(up.get("decoded_payload") or {}) if (dev, port) in FIX_PORTS else None
        for g in up.get("rx_metadata") or []:
            k, name = gw_kennung(g)
            e = gw.setdefault(k, dict(name=name, n=0, erst=ts, letzt=ts, rssi=-999, snr=None,
                                      geraete=set(), ort=None, fixe=[]))
            e["n"] += 1
            e["letzt"] = ts
            e["geraete"].add(dev)
            r = g.get("rssi", g.get("channel_rssi"))
            if r is not None and r > e["rssi"]:
                e["rssi"], e["snr"] = r, g.get("snr")
            loc = g.get("location") or {}
            if loc.get("latitude") and loc.get("longitude"):
                e["ort"] = (loc["latitude"], loc["longitude"])
            if fix and r is not None:
                e["fixe"].append((fix[0], fix[1], r))

    for k, (name, la, lo) in GATEWAYS.items():
        if k in gw and not gw[k]["ort"]:
            gw[k]["ort"] = (la, lo)

    orte = Orte(a.ohne_ort)
    zeitraum = f"letzte {a.tage} Tage" if a.tage else "gesamter Bestand"
    print(f"Gateways aus wagodb.loradevice (TTN-Uplinks, {zeitraum}"
          f"{', ' + a.geraet if a.geraet else ''}): {len(gw)} Gateways, {len(zeilen)} Uplinks\n")
    kopf = f"{'Gateway':34s} {'Empf':>5s} {'best':>9s} {'Fixe':>4s}  {'Standort':24s} {'Reichw':>7s}  {'zuletzt':16s}  Ort"
    print(kopf)
    print("-" * (len(kopf) + 20))
    for k, e in sorted(gw.items(), key=lambda x: -x[1]["n"]):
        fixe = e["fixe"]
        reichw = ""
        if e["ort"]:
            la, lo = e["ort"]
            art = "reg"
            if fixe:
                reichw = f"{max(km(la, lo, f[0], f[1]) for f in fixe):5.1f}km"
        elif fixe:
            w = [10 ** (f[2] / 10) for f in fixe]
            la = sum(f[0] * x for f, x in zip(fixe, w)) / sum(w)
            lo = sum(f[1] * x for f, x in zip(fixe, w)) / sum(w)
            art = "~"
        else:
            la = lo = None
            art = "?"
        pos = f"{art:3s} {la:8.4f},{lo:8.4f}" if la is not None else f"{art:3s} unbekannt"
        ort = orte.name(la, lo) if la is not None else ""
        pegel = f"{e['rssi']}/{e['snr']:+.0f}" if e["snr"] is not None else f"{e['rssi']}"
        print(f"{e['name'][:34]:34s} {e['n']:5d} {pegel:>9s} {len(fixe):4d}  {pos:24s} {reichw:>7s}  "
              f"{e['letzt']:%d.%m.%y %H:%M}    {ort}")
    print("\nbest = staerkster RSSI dBm / SNR dB; Fixe = Empfaenge mit frischem Geraete-Fix;"
          "\nreg = Position von TTS/eigener Liste, ~ = aus Fixen geschaetzt (s. Kopf der Datei);"
          "\nReichw = groesste Entfernung Geraet->Gateway bei Empfang (nur bei bekanntem Standort).")


if __name__ == "__main__":
    main()
