#!/usr/bin/env python3
"""freetz_nodes2db.py — welche WLAN-Nodes haengen noch an der Freetz 7240?

Stuendlich per cron. Holt in EINEM SSH-Aufruf die Stationsliste des AP f7240
(192.168.178.26) und dessen ARP-Tabelle, und schreibt beides zusammengefuehrt
nach wagodb.freetz_nodes. Kein MQTT dazwischen: der Pi ist Datenhalter und
Schreiber in einem, ein Broker-Hop wuerde nur eine weitere Ausfallstelle
zwischen Messung und Tabelle setzen.

Zweck: der ESP32-S3 (sofar-waveshare) haengt an genau diesem AP und verschwand
nach Tagen von dort. Die Tabelle beantwortet hinterher, wann er wegging und
ob er der einzige war -- oder ob der AP alle verloren hat.

Quelle: `wlanconfig ath0 list` (Atheros, nur 2,4 GHz; ath1 existiert nicht).
        RSSI ist dort SNR in dB, nicht dBm.
"""
import re
import socket
import subprocess
import sys

import pymysql

FREETZ = "freetz"          # ~/.ssh/config: 192.168.178.26, root, id_ed25519_freetz
IFACES = ("ath0",)
DB = dict(host="192.168.178.218", user="gh", password="a12345",
          database="wagodb", charset="utf8mb4")

DDL = """
CREATE TABLE IF NOT EXISTS freetz_nodes (
  id       INT AUTO_INCREMENT PRIMARY KEY,
  ts       DATETIME     NOT NULL,
  iface    VARCHAR(8)   NOT NULL,
  mac      VARCHAR(17)  NOT NULL,
  ip       VARCHAR(45)  NULL,
  hostname VARCHAR(64)  NULL,
  aid      INT          NULL,
  chan     INT          NULL,
  rate     VARCHAR(8)   NULL,
  rssi     INT          NULL,   -- SNR in dB, wie wlanconfig es liefert
  idle     INT          NULL,
  KEY idx_ts (ts),
  KEY idx_mac_ts (mac, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

MAC_RE = re.compile(r"^([0-9a-f]{2}(?::[0-9a-f]{2}){5})\s+(.*)$", re.I)
# In /proc/net/arp steht die MAC als eigenes Feld ohne Rest dahinter -- MAC_RE
# mit seinem \s+(.*) greift dort also nicht.
MAC_ONLY = re.compile(r"^[0-9a-f]{2}(?::[0-9a-f]{2}){5}$", re.I)
NULL_MAC = "00:00:00:00:00:00"


def remote():
    """Stationsliste + ARP in einem Rutsch holen; Abschnitte per Marker."""
    cmd = ["echo '#ARP'", "cat /proc/net/arp"]
    for i in IFACES:
        cmd = [f"echo '#IF {i}'", f"wlanconfig {i} list 2>/dev/null"] + cmd
    out = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", FREETZ, "; ".join(cmd)],
        capture_output=True, text=True, timeout=25)
    if out.returncode != 0:
        raise RuntimeError(f"ssh {FREETZ}: rc={out.returncode} {out.stderr.strip()[:120]}")
    return out.stdout


def parse(text):
    """-> (stations, arp). wlanconfig-Spalten: ADDR AID CHAN RATE RSSI IDLE ..."""
    stations, arp, iface = [], {}, None
    mode = None
    for line in text.splitlines():
        if line.startswith("#IF "):
            mode, iface = "if", line[4:].strip()
            continue
        if line.startswith("#ARP"):
            mode = "arp"
            continue
        if mode == "if":
            m = MAC_RE.match(line.strip())
            if not m:
                continue          # Kopfzeile ADDR AID ...
            f = m.group(2).split()
            def num(i):
                try:
                    return int(f[i])
                except (IndexError, ValueError):
                    return None
            stations.append(dict(iface=iface, mac=m.group(1).lower(),
                                 aid=num(0), chan=num(1),
                                 rate=f[2] if len(f) > 2 else None,
                                 rssi=num(3), idle=num(4)))
        elif mode == "arp":
            f = line.split()
            if len(f) >= 4 and MAC_ONLY.match(f[3]) and f[3] != NULL_MAC:
                arp[f[3].lower()] = f[0]
    return stations, arp


def local_arp():
    """ARP-Tabelle des Pi als zweite Quelle.

    Die Freetz kennt nur, womit sie selbst zuletzt gesprochen hat -- vom ESP32
    steht dort meist nichts, obwohl er assoziiert ist. Der Pi dagegen ist sein
    MQTT-Broker und hat ihn deshalb im ARP-Cache.
    """
    arp = {}
    try:
        with open("/proc/net/arp") as fh:
            for line in fh:
                f = line.split()
                if len(f) >= 4 and MAC_ONLY.match(f[3]) and f[3] != NULL_MAC:
                    arp[f[3].lower()] = f[0]
    except OSError:
        pass
    return arp


def hostname(ip):
    """Best effort ueber den DNS der Fritzbox; ein Node ohne Namen ist kein Fehler."""
    if not ip:
        return None
    try:
        socket.setdefaulttimeout(2)
        return socket.gethostbyaddr(ip)[0].split(".")[0][:64]
    except OSError:
        return None


def main():
    quiet = "--quiet" in sys.argv
    stations, arp = parse(remote())
    here = local_arp()
    for s in stations:
        s["ip"] = arp.get(s["mac"]) or here.get(s["mac"])
        s["hostname"] = hostname(s["ip"])

    con = pymysql.connect(**DB)
    try:
        with con.cursor() as cur:
            cur.execute(DDL)
            cur.executemany(
                "INSERT INTO freetz_nodes"
                " (ts, iface, mac, ip, hostname, aid, chan, rate, rssi, idle)"
                " VALUES (NOW(), %(iface)s, %(mac)s, %(ip)s, %(hostname)s,"
                " %(aid)s, %(chan)s, %(rate)s, %(rssi)s, %(idle)s)", stations)
        con.commit()
    finally:
        con.close()

    if not quiet:
        for s in stations:
            print(f"{s['iface']} {s['mac']} {s['ip'] or '-':15} "
                  f"{s['hostname'] or '-':14} rssi={s['rssi']} idle={s['idle']}")
    print(f"{len(stations)} Nodes an f7240 -> wagodb.freetz_nodes")


if __name__ == "__main__":
    main()
