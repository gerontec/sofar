#!/usr/bin/env python3
"""freetz_nodes2db.py — welche WLAN-Nodes haengen noch an der Freetz 7240?

Alle 30 min per cron (7,37). Holt in EINEM SSH-Aufruf die Stationsliste des AP f7240
(192.168.178.26) und dessen ARP-Tabelle, und schreibt beides zusammengefuehrt
nach wagodb.freetz_nodes. Kein MQTT dazwischen: der Pi ist Datenhalter und
Schreiber in einem, ein Broker-Hop wuerde nur eine weitere Ausfallstelle
zwischen Messung und Tabelle setzen.

Zweck: der ESP32-S3 (sofar-waveshare) haengt an genau diesem AP und verschwand
nach Tagen von dort. Die Tabelle beantwortet hinterher, wann er wegging und
ob er der einzige war -- oder ob der AP alle verloren hat.

Quelle: `wlanconfig ath0 list` (Atheros, nur 2,4 GHz; ath1 existiert nicht).
        RSSI ist dort SNR in dB, nicht dBm.

Zum "last active": die Box selbst kennt keinen solchen Zeitstempel. Am
21.08.2026 geprueft -- die landevices in /var/flash/ar7.cfg haben kein
Zeitfeld (nur ip/mac/medium/type), ctlmgr_ctl kennt weder last_active noch
last_seen, /var/flash/multid.leases ist leer (die Box ist nicht der
DHCP-Server, das ist die 6490), und die IDLE-Spalte von wlanconfig zaehlt
nicht hoch: sie stand in zwei Messungen 8 s auseinander unveraendert auf
120/135/150 und ist damit der eingestellte Inaktivitaets-Timeout, nicht die
Zeit seit dem letzten Frame.

Zur IPv6-Spalte: die Freetz kennt keinen Nachbar-Cache im Dateisystem (kein
/proc/net/ndisc_cache, kein `ip`-Binary), und die ARP-Tabelle ist reines IPv4.
Die Adressen kommen deshalb vom Pi selbst: ein Ping auf ff02::1 holt die
Link-Local-Adressen aller Nodes im Segment, und zu jedem globalen Praefix des
LAN-Interfaces wird zusaetzlich die EUI-64-Adresse der MAC angepingt -- nicht
wegen der Antwort, sondern weil die Neighbor Solicitation den Cache fuellt.
Gespeichert wird die beste Adresse je Node: globale EUI-64 vor sonstiger
globaler vor ULA vor Link-Local.

Deshalb fuehrt dieses Skript den Stempel selbst: freetz_node_last haelt je MAC
first_seen/last_seen/seen_count fort. Aufloesung = Cron-Takt (30 min).
"""
import ipaddress
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
  ipv6     VARCHAR(45)  NULL,
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

DDL_LAST = """
CREATE TABLE IF NOT EXISTS freetz_node_last (
  mac        VARCHAR(17)  NOT NULL PRIMARY KEY,
  ip         VARCHAR(45)  NULL,
  ipv6       VARCHAR(45)  NULL,
  hostname   VARCHAR(64)  NULL,
  first_seen DATETIME     NOT NULL,
  last_seen  DATETIME     NOT NULL,   -- "zuletzt aktiv", vom Poller gefuehrt
  last_rssi  INT          NULL,
  seen_count INT          NOT NULL DEFAULT 1,
  KEY idx_last_seen (last_seen)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

DDL_HEALTH = """
CREATE TABLE IF NOT EXISTS freetz_health (
  id            INT AUTO_INCREMENT PRIMARY KEY,
  ts            DATETIME NOT NULL,
  uptime_s      INT      NULL,   -- faellt = Box war neu gestartet, Zaehler bei 0
  load1         FLOAT    NULL,
  mem_free      INT      NULL,   -- kB
  mem_cached    INT      NULL,
  mem_buffers   INT      NULL,
  swap_free     INT      NULL,   -- unangetastet = kein Speicherdruck
  ath_tx_timeout    INT NULL,    -- athstats: global transmit timeout interrupts
  ath_cs_timeout    INT NULL,    -- athstats: carrier sense timeout interrupts
  ath_beacon_stuck  INT NULL,    -- Vorbote eines Atheros-Radio-Hangs
  ath_eol           INT NULL,    -- recv eol interrupts
  ath_long_retry    FLOAT NULL,  -- tx unaggregated long retry percent
  mac_tx_no_node    INT NULL,    -- 80211stats: Frames an Stationen, die weg sind
  mac_deauth        INT NULL,
  mac_disassoc      INT NULL,
  mac_inact_timeout INT NULL,    -- nodes timed out inactivity
  ath0_rx_bytes BIGINT NULL,
  ath0_tx_bytes BIGINT NULL,
  ath0_rx_drop  INT    NULL,
  ath0_tx_drop  INT    NULL,     -- deckt sich mit mac_tx_no_node
  KEY idx_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

MAC_RE = re.compile(r"^([0-9a-f]{2}(?::[0-9a-f]{2}){5})\s+(.*)$", re.I)
# In /proc/net/arp steht die MAC als eigenes Feld ohne Rest dahinter -- MAC_RE
# mit seinem \s+(.*) greift dort also nicht.
MAC_ONLY = re.compile(r"^[0-9a-f]{2}(?::[0-9a-f]{2}){5}$", re.I)
NULL_MAC = "00:00:00:00:00:00"


def remote():
    """Stationsliste + ARP in einem Rutsch holen; Abschnitte per Marker."""
    # athstats/80211stats liegen nicht im PATH der Box -> voller Pfad.
    cmd = ["echo '#ARP'", "cat /proc/net/arp",
           "echo '#UPTIME'", "cat /proc/uptime",
           "echo '#LOAD'", "cat /proc/loadavg",
           "echo '#MEM'", "cat /proc/meminfo",
           "echo '#DEV'", "cat /proc/net/dev",
           "echo '#ATH'", "/usr/sbin/athstats -i wifi0 2>/dev/null",
           "echo '#MAC'", "/usr/sbin/80211stats -i ath0 2>/dev/null"]
    for i in IFACES:
        cmd = [f"echo '#IF {i}'", f"wlanconfig {i} list 2>/dev/null"] + cmd
    out = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", FREETZ, "; ".join(cmd)],
        capture_output=True, text=True, timeout=25)
    if out.returncode != 0:
        raise RuntimeError(f"ssh {FREETZ}: rc={out.returncode} {out.stderr.strip()[:120]}")
    return out.stdout


# athstats/80211stats geben pro Zeile "<zahl> <klartext>" aus -- und zwar NUR
# fuer Zaehler ungleich 0. Ein fehlendes Label heisst also 0, nicht "unbekannt".
STAT_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s+(\S.*?)\s*$")


def parse(text):
    """-> (stations, arp, health). wlanconfig: ADDR AID CHAN RATE RSSI IDLE ..."""
    stations, arp, iface = [], {}, None
    mem, stat, dev = {}, {}, {}
    health = {}
    mode = None
    for line in text.splitlines():
        if line.startswith("#IF "):
            mode, iface = "if", line[4:].strip()
            continue
        if line.startswith("#"):
            mode = line[1:].strip().lower()
            continue
        if mode == "uptime":
            try:
                health["uptime_s"] = int(float(line.split()[0]))
            except (IndexError, ValueError):
                pass
            continue
        if mode == "load":
            try:
                health["load1"] = float(line.split()[0])
            except (IndexError, ValueError):
                pass
            continue
        if mode == "mem":
            f = line.split()
            if len(f) >= 2 and f[0].endswith(":"):
                try:
                    mem[f[0][:-1]] = int(f[1])
                except ValueError:
                    pass
            continue
        if mode == "dev":
            if ":" not in line:
                continue
            name, _, rest = line.partition(":")
            f = rest.split()
            if len(f) >= 12:
                dev[name.strip()] = f
            continue
        if mode in ("ath", "mac"):
            m = STAT_RE.match(line)
            if m:
                stat.setdefault(m.group(2), m.group(1))
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

    def cnt(label, cast=int):
        try:
            return cast(stat[label])
        except (KeyError, ValueError):
            return 0

    health.update(
        mem_free=mem.get("MemFree"), mem_cached=mem.get("Cached"),
        mem_buffers=mem.get("Buffers"), swap_free=mem.get("SwapFree"),
        ath_tx_timeout=cnt("global transmit timeout interrupts"),
        ath_cs_timeout=cnt("carrier sense timeout interrupts"),
        ath_beacon_stuck=cnt("beacon stuck"),
        ath_eol=cnt("recv eol interrupts"),
        ath_long_retry=cnt("tx unaggregated long retry percent", float),
        mac_tx_no_node=cnt("tx failed for no node"),
        mac_deauth=cnt("rx deauthentication"),
        mac_disassoc=cnt("rx disassociation"),
        mac_inact_timeout=cnt("nodes timed out inactivity"))
    d = dev.get("ath0")
    if d:
        health.update(ath0_rx_bytes=int(d[0]), ath0_rx_drop=int(d[3]),
                      ath0_tx_bytes=int(d[8]), ath0_tx_drop=int(d[11]))
    for k in ("uptime_s", "load1", "ath0_rx_bytes", "ath0_rx_drop",
              "ath0_tx_bytes", "ath0_tx_drop"):
        health.setdefault(k, None)
    return stations, arp, health


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


LAN_PROBE = "192.168.178.26"      # die Freetz selbst -- nur, um das LAN-Interface zu finden


def lan_iface():
    """Interface, ueber das der Pi das Heimsegment sieht (nicht tun0)."""
    try:
        out = subprocess.run(["ip", "-o", "route", "get", LAN_PROBE],
                             capture_output=True, text=True, timeout=5).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    m = re.search(r"\bdev\s+(\S+)", out)
    return m.group(1) if m else None


def eui64(mac):
    """MAC -> Interface-ID nach RFC 4291 (Bit 7 des ersten Bytes kippen)."""
    try:
        b = [int(x, 16) for x in mac.split(":")]
    except ValueError:
        return None
    if len(b) != 6:
        return None
    return "%02x%02x:%02xff:fe%02x:%02x%02x" % (b[0] ^ 0x02, b[1], b[2],
                                                b[3], b[4], b[5])


def prefixes(iface):
    """Globale /64-Praefixe (GUA und ULA) des LAN-Interfaces."""
    out = subprocess.run(["ip", "-o", "-6", "addr", "show", "dev", iface],
                         capture_output=True, text=True, timeout=5).stdout
    nets = []
    for m in re.finditer(r"inet6\s+(\S+/\d+)", out):
        try:
            net = ipaddress.ip_network(m.group(1), strict=False)
        except ValueError:
            continue
        if net.prefixlen == 64 and not net.network_address.is_link_local \
                and not net.network_address.is_loopback and net not in nets:
            nets.append(net)
    return nets


def neigh(iface):
    """MAC -> Liste bekannter IPv6-Adressen aus dem Neighbor-Cache."""
    out = subprocess.run(["ip", "-6", "neigh", "show", "dev", iface],
                         capture_output=True, text=True, timeout=5).stdout
    found = {}
    for line in out.splitlines():
        f = line.split()
        if len(f) < 3 or "lladdr" not in f:
            continue          # INCOMPLETE/FAILED: Adresse hat nicht geantwortet
        if "FAILED" in f or "INCOMPLETE" in f:
            continue
        mac = f[f.index("lladdr") + 1].lower()
        found.setdefault(mac, []).append(f[0])
    return found


def rank(addr, mac):
    """Kleiner ist besser: globale EUI-64 < globale < ULA < Link-Local."""
    try:
        a = ipaddress.IPv6Address(addr)
    except ValueError:
        return 9
    if a.is_link_local:
        return 3
    if a in ipaddress.ip_network("fc00::/7"):
        return 2
    iid = eui64(mac)
    return 0 if iid and addr.lower().endswith(iid) else 1


def ipv6_map(macs):
    """Nachbarn anstupsen und je MAC die beste IPv6 zurueckgeben.

    Der Ping ist Mittel zum Zweck: uns interessiert nicht die Antwort, sondern
    dass die Neighbor Solicitation den Cache fuellt. Deshalb laufen alle Pings
    parallel und mit kurzem Timeout -- ein stiller Node kostet keine Zeit.
    """
    iface = lan_iface()
    if not iface:
        return {}
    targets = ["ff02::1%" + iface]
    for net in prefixes(iface):
        for mac in macs:
            iid = eui64(mac)
            if iid:
                targets.append(str(ipaddress.IPv6Address(
                    int(net.network_address) | int(ipaddress.IPv6Address("::" + iid)))))
    procs = [subprocess.Popen(["ping6", "-c", "2", "-W", "1", "-i", "0.3", t],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
             for t in targets]
    for pr in procs:
        try:
            pr.wait(timeout=4)
        except subprocess.TimeoutExpired:
            pr.kill()
    table = neigh(iface)
    best = {}
    for mac in macs:
        addrs = table.get(mac.lower())
        if addrs:
            best[mac] = sorted(set(addrs), key=lambda a: (rank(a, mac), a))[0][:45]
    return best


def ensure_column(cur, table, col, spec):
    """CREATE TABLE IF NOT EXISTS ruehrt bestehende Tabellen nicht an."""
    cur.execute("SELECT COUNT(*) FROM information_schema.columns"
                " WHERE table_schema = DATABASE() AND table_name = %s"
                " AND column_name = %s", (table, col))
    if not cur.fetchone()[0]:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {spec}")


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
    stations, arp, health = parse(remote())
    here = local_arp()
    v6 = ipv6_map([s["mac"] for s in stations])
    for s in stations:
        s["ip"] = arp.get(s["mac"]) or here.get(s["mac"])
        s["ipv6"] = v6.get(s["mac"])
        s["hostname"] = hostname(s["ip"])

    con = pymysql.connect(**DB)
    try:
        with con.cursor() as cur:
            cur.execute(DDL)
            cur.execute(DDL_LAST)
            cur.execute(DDL_HEALTH)
            ensure_column(cur, "freetz_nodes", "ipv6", "VARCHAR(45) NULL AFTER ip")
            ensure_column(cur, "freetz_node_last", "ipv6", "VARCHAR(45) NULL AFTER ip")
            cur.executemany(
                "INSERT INTO freetz_nodes"
                " (ts, iface, mac, ip, ipv6, hostname, aid, chan, rate, rssi, idle)"
                " VALUES (NOW(), %(iface)s, %(mac)s, %(ip)s, %(ipv6)s, %(hostname)s,"
                " %(aid)s, %(chan)s, %(rate)s, %(rssi)s, %(idle)s)", stations)
            # last_seen fortschreiben. COALESCE haelt eine einmal ermittelte IP
            # fest: sie kommt aus der ARP-Tabelle und fehlt mal, wenn gerade
            # niemand mit dem Node gesprochen hat -- das ist kein Grund, den
            # Namen wieder zu verlieren.
            cur.executemany(
                "INSERT INTO freetz_node_last"
                " (mac, ip, ipv6, hostname, first_seen, last_seen, last_rssi,"
                " seen_count)"
                " VALUES (%(mac)s, %(ip)s, %(ipv6)s, %(hostname)s, NOW(), NOW(),"
                " %(rssi)s, 1)"
                " ON DUPLICATE KEY UPDATE"
                "  last_seen = NOW(),"
                "  ip = COALESCE(VALUES(ip), ip),"
                "  ipv6 = COALESCE(VALUES(ipv6), ipv6),"
                "  hostname = COALESCE(VALUES(hostname), hostname),"
                "  last_rssi = VALUES(last_rssi),"
                "  seen_count = seen_count + 1", stations)
            cols = sorted(health)
            cur.execute(
                "INSERT INTO freetz_health (ts, " + ", ".join(cols) + ")"
                " VALUES (NOW(), " + ", ".join(f"%({c})s" for c in cols) + ")",
                health)
        con.commit()
    finally:
        con.close()

    if not quiet:
        for s in stations:
            print(f"{s['iface']} {s['mac']} {s['ip'] or '-':15} "
                  f"{s['ipv6'] or '-':39} "
                  f"{s['hostname'] or '-':14} rssi={s['rssi']} idle={s['idle']}")
    if not quiet:
        print(f"health: up={health['uptime_s']}s free={health['mem_free']}kB "
              f"swapfree={health['swap_free']}kB beaconstuck={health['ath_beacon_stuck']} "
              f"txnonode={health['mac_tx_no_node']} txdrop={health['ath0_tx_drop']}")
    print(f"{len(stations)} Nodes an f7240 -> wagodb.freetz_nodes"
          " + freetz_node_last + freetz_health")


if __name__ == "__main__":
    main()
