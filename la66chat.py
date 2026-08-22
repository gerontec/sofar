#!/usr/bin/env python3
"""Chat ueber den Dragino LA66 oder den Pico-Knoten — jede getippte Zeile geht
per LoRaWAN raus und wird zusaetzlich in der MariaDB auf 192.168.5.23 abgelegt.

    LA66 (USB hier)  ->  Gateway Lenggries  ->  ChirpStack auf dem dell
    getippte Zeile   ->  MariaDB 192.168.5.23, wagodb.la66_chat

Die Datenbank ist der verlaessliche Teil: geschrieben wird VOR dem Senden und
unabhaengig davon, ob der Funkweg klappt. Das Sendeergebnis wird danach in
derselben Zeile nachgetragen (txDone / kein txDone / Fehlertext). Faellt das
Netz zur Datenbank aus, laeuft der Chat trotzdem weiter -- die Zeile wird dann
als ungespeichert markiert ausgegeben, damit nichts stillschweigend verloren
geht.

Alles am Funk (AT-Dialekt, Rahmenkopf, Nutzlastgrenzen, Duty-Cycle-Pause) kommt
unveraendert aus dragino.py -- eine zweite Variante derselben Logik waere genau
die Stelle, an der die beiden auseinanderlaufen.

Als Sendestation taugt jedes Geraet, das den LA66-AT-Dialekt spricht. Der
Pico-Knoten (e22pico) tut das absichtlich -- er kennt AT+SENDB, AT+DR, AT+DEUI
und antwortet auf AT+LORAWAN, was der LA66 nicht kennt; daran werden die beiden
unterschieden. Zwei Unterschiede bleiben und werden hier behandelt:

  Baudrate      LA66 9600 (CP2102), Pico 115200 (natives USB-CDC).
                Vorgabe richtet sich nach dem Portnamen, --baud sticht.
  Erfolgswort   Der LA66 meldet txDone, wenn der Rahmen wirklich draussen ist;
                der Pico gibt schlicht OK aus -- aber erst, nachdem
                sendReceive() samt beider Empfangsfenster durch ist, es ist
                also dieselbe Aussage. Ein frueher OK-Treffer beim LA66 waere
                dagegen falsch, deshalb je Geraet ein eigenes Wort.

    ./la66chat.py                          # LA66 auf /dev/ttyUSB0, DR3
    ./la66chat.py --port /dev/ttyACM0      # Pico, Baud und Erfolgswort automatisch
    ./la66chat.py --dr 0                   # groesste Reichweite, 51 Byte je Rahmen
    ./la66chat.py --nur-db                 # nur mitschreiben, nicht senden

Im Chat:  /dr <0-5>   Datenrate wechseln
          /stat       Zaehler und Verbindungszustand
          /quit       Ende (auch Strg-D)
"""
import argparse
import datetime as dt
import random
import select
import sys

sys.path.insert(0, "/home/gh/python")
import pymysql                                             # noqa: E402
from dragino import (AIRTIME_S, FPORT, MAX_PAYLOAD, PORT,  # noqa: E402
                     at, chunks, open_port)

DB = {"host": "192.168.5.23", "db": "wagodb", "user": "gh", "pw": "a12345"}

TABELLE = """
CREATE TABLE IF NOT EXISTS la66_chat (
  id        INT AUTO_INCREMENT PRIMARY KEY,
  ts        DATETIME     NOT NULL,
  richtung  VARCHAR(3)   NOT NULL,          -- tx = getippt, rx = empfangen
  text      VARCHAR(512) NOT NULL,
  bytes     SMALLINT     DEFAULT NULL,      -- Nutzlast in Byte
  rahmen    SMALLINT     DEFAULT NULL,      -- in so viele Teile zerlegt
  dr        TINYINT      DEFAULT NULL,
  ergebnis  VARCHAR(64)  DEFAULT NULL,      -- txDone, unbestaetigt, Fehler
  dev_eui   VARCHAR(16)  DEFAULT NULL,
  KEY (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def db_verbinden():
    return pymysql.connect(host=DB["host"], database=DB["db"], user=DB["user"],
                           password=DB["pw"], connect_timeout=5, autocommit=True,
                           charset="utf8mb4")


def geraet_erkennen(ser):
    """(Name, Erfolgswort) -- AT+LORAWAN kennt nur der Pico-Knoten."""
    r = at(ser, "AT+LORAWAN=?", wait=0.8)
    hat_antwort = any(z.strip() in ("0", "1") for z in r.splitlines())
    if hat_antwort and "ERROR" not in r.upper():
        return "pico", "OK"
    return "la66", "txDone"


def dev_eui(ser):
    """DevEUI einmal abfragen, damit in der DB steht, welcher Knoten getippt hat."""
    r = at(ser, "AT+DEUI=?", wait=0.8)
    for zeile in r.splitlines():
        z = zeile.strip().replace(" ", "")
        if len(z) == 16 and all(c in "0123456789ABCDEFabcdef" for c in z):
            return z.upper()
    return None


class Chat:
    def __init__(self, args):
        self.args = args
        self.dr = args.dr
        self.conn = None
        self.db_fehler = 0
        self.gesendet = 0
        self.ser = None
        self.eui = None
        self.geraet = "?"
        self.erfolgswort = "txDone"

    # --- Datenbank ----------------------------------------------------------
    def db(self):
        if self.conn is None:
            self.conn = db_verbinden()
            with self.conn.cursor() as cur:
                cur.execute(TABELLE)
        return self.conn

    def schreiben(self, text, bytes_, rahmen):
        """Zeile ablegen und die id zurueckgeben; None, wenn die DB nicht will."""
        try:
            with self.db().cursor() as cur:
                cur.execute(
                    "INSERT INTO la66_chat (ts,richtung,text,bytes,rahmen,dr,dev_eui) "
                    "VALUES (%s,'tx',%s,%s,%s,%s,%s)",
                    (dt.datetime.now(), text, bytes_, rahmen, self.dr, self.eui))
                return cur.lastrowid
        except Exception as e:
            self.conn = None
            self.db_fehler += 1
            print(f"  ! NICHT gespeichert ({e.__class__.__name__}: {e})")
            return None

    def nachtragen(self, zeilen_id, ergebnis):
        if zeilen_id is None:
            return
        try:
            with self.db().cursor() as cur:
                cur.execute("UPDATE la66_chat SET ergebnis=%s WHERE id=%s",
                            (ergebnis[:64], zeilen_id))
        except Exception as e:
            self.conn = None
            print(f"  ! Ergebnis nicht nachgetragen ({e})")

    # --- Funk ---------------------------------------------------------------
    def senden(self, text):
        """Wie dragino.py: 2 Byte Kopf, zerlegen, je Rahmen AT+SENDB."""
        body = text.encode("utf-8")
        per_frame = MAX_PAYLOAD[self.dr] - 2
        parts = chunks(body, per_frame)
        msg_id = random.randint(0, 255)
        ergebnisse = []
        for i, part in enumerate(parts):
            last = 0x80 if i == len(parts) - 1 else 0x00
            frame = bytes([msg_id, last | i]) + part
            cmd = (f"AT+SENDB={1 if self.args.confirm else 0},{FPORT},"
                   f"{len(frame)},{frame.hex().upper()}")
            out = at(self.ser, cmd, wait=60, collect=self.erfolgswort)
            ok = self.erfolgswort in out
            # Das Erfolgswort des Geraets mitschreiben, nicht ein festes:
            # der LA66 sagt txDone, der Pico OK -- in der DB soll stehen, was
            # tatsaechlich bestaetigt wurde.
            ergebnisse.append(self.erfolgswort if ok else "unbestaetigt")
            print(f"  Rahmen {i + 1}/{len(parts)}: {len(frame)} B "
                  f"{'gesendet' if ok else 'UNBESTAETIGT'}")
            if not ok:
                print("   ", out.strip()[:160])
            if i < len(parts) - 1:
                pause = AIRTIME_S[self.dr] * 100      # 1 % Duty Cycle
                print(f"    warte {pause:.0f}s (Duty Cycle)")
                self.warten(pause)
        self.gesendet += 1
        return ",".join(sorted(set(ergebnisse))), len(parts)

    def warten(self, sekunden):
        """Warten, aber weiter mitlesen -- Downlinks kommen sonst erst spaeter."""
        ende = select.select
        import time
        t0 = time.time()
        while time.time() - t0 < sekunden:
            r, _, _ = ende([self.ser], [], [], 0.5)
            if r:
                self.lesen()

    def lesen(self):
        roh = self.ser.read(4096)
        if not roh:
            return
        for zeile in roh.decode("utf-8", "replace").splitlines():
            if zeile.strip():
                print(f"  < {zeile.strip()}")

    # --- Ablauf -------------------------------------------------------------
    def lauf(self):
        baud = self.args.baud or (115200 if "ACM" in self.args.port else 9600)
        self.ser = open_port(self.args.port, baud)
        self.geraet, self.erfolgswort = geraet_erkennen(self.ser)
        self.eui = dev_eui(self.ser)
        r = at(self.ser, f"AT+DR={self.dr}")
        if "OK" not in r:
            print(f"Warnung: Datenrate nicht bestaetigt — {r.strip()!r}")
        try:
            self.db()
            db_zustand = f"{DB['host']}/{DB['db']}.la66_chat bereit"
        except Exception as e:
            db_zustand = f"DB NICHT erreichbar ({e})"
        print(f"{self.geraet.upper()} {self.eui or '?'} auf {self.args.port} "
              f"@{baud}, DR{self.dr} ({MAX_PAYLOAD[self.dr]} B/Rahmen), "
              f"Erfolg = {self.erfolgswort}")
        print(f"Datenbank: {db_zustand}")
        print("Tippen und Enter sendet. /dr <n>, /stat, /quit\n")

        while True:
            r, _, _ = select.select([sys.stdin, self.ser], [], [], 0.5)
            if self.ser in r:
                self.lesen()
            if sys.stdin not in r:
                continue
            zeile = sys.stdin.readline()
            if not zeile:                      # Strg-D
                break
            zeile = zeile.rstrip("\n")
            if not zeile.strip():
                continue
            if zeile.startswith("/"):
                if not self.befehl(zeile):
                    break
                continue

            body = zeile.encode("utf-8")
            teile = -(-len(body) // (MAX_PAYLOAD[self.dr] - 2))
            zeilen_id = self.schreiben(zeile, len(body), teile)
            if zeilen_id:
                print(f"  > in DB (id {zeilen_id}), {len(body)} B in {teile} Rahmen")
            if self.args.nur_db:
                self.nachtragen(zeilen_id, "nur-db")
                continue
            try:
                ergebnis, rahmen = self.senden(zeile)
            except Exception as e:
                ergebnis = f"Sendefehler: {e}"
                print("  !", ergebnis)
            self.nachtragen(zeilen_id, ergebnis)

    def befehl(self, zeile):
        teile = zeile.split()
        if teile[0] in ("/quit", "/exit"):
            return False
        if teile[0] == "/dr" and len(teile) == 2 and teile[1].isdigit():
            neu = int(teile[1])
            if neu in MAX_PAYLOAD:
                self.dr = neu
                r = at(self.ser, f"AT+DR={neu}")
                print(f"  DR{neu} ({MAX_PAYLOAD[neu]} B/Rahmen) "
                      f"{'gesetzt' if 'OK' in r else 'NICHT bestaetigt'}")
            else:
                print("  DR 0..5")
            return True
        if teile[0] == "/stat":
            print(f"  {self.geraet}, gesendet: {self.gesendet}, "
                  f"DB-Fehler: {self.db_fehler}, DR{self.dr}, DevEUI {self.eui}")
            try:
                with self.db().cursor() as cur:
                    cur.execute("SELECT COUNT(*), MAX(ts) FROM la66_chat")
                    n, letzte = cur.fetchone()
                print(f"  in der DB: {n} Zeilen, zuletzt {letzte}")
            except Exception as e:
                print(f"  DB nicht erreichbar: {e}")
            return True
        print("  bekannt sind /dr <0-5>, /stat, /quit")
        return True


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dr", type=int, default=3, choices=sorted(MAX_PAYLOAD),
                    help="Datenrate: 0=SF12 groesste Reichweite, 5=SF7 (Vorgabe 3)")
    ap.add_argument("--port", default=PORT, help=f"serieller Port (Vorgabe {PORT})")
    ap.add_argument("--baud", type=int, default=None,
                    help="Baudrate; ohne Angabe 115200 fuer ttyACM*, sonst 9600")
    ap.add_argument("--confirm", action="store_true",
                    help="bestaetigte Uplinks anfordern")
    ap.add_argument("--nur-db", action="store_true",
                    help="nur in die Datenbank schreiben, nicht funken")
    args = ap.parse_args()
    chat = Chat(args)
    try:
        chat.lauf()
    except KeyboardInterrupt:
        pass
    finally:
        if chat.ser:
            chat.ser.close()
        print("\nEnde.")


if __name__ == "__main__":
    main()
