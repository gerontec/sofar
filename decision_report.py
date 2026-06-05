#!/usr/bin/env python3
"""fox2db Entscheidungs-Report — führt decision.sql aus und formatiert die Ergebnisse.

decision.sql bleibt Single Source of Truth; dieses Script parst die nummerierten
Abschnitte, führt jede Query gegen MariaDB aus und gibt sie als Tabelle aus.

Nutzung:  python3 decision_report.py [--query N] [--width N]
"""

import re
import sys
from pathlib import Path

import pymysql

DB = dict(host="192.168.178.218", user="gh", password="a12345",
          database="wagodb", connect_timeout=5)

SQL_FILE = Path(__file__).parent / "decision.sql"

# Spalten, die für die Lesbarkeit gekürzt werden (lange Texte)
MAXCOL = {"erklaerung": 70, "detail": 80, "reason": 50}


def parse_queries(path: Path):
    """Zerlegt decision.sql in (nummer, titel, sql) — Titel aus '-- N) ...'."""
    text = path.read_text(encoding="utf-8")
    blocks = []
    for stmt in text.split(";"):
        if not stmt.strip():
            continue
        title, num = None, None
        sql_lines = []
        for line in stmt.splitlines():
            s = line.strip()
            m = re.match(r"--\s*(\d+)\)\s*(.+)", s)
            if m:
                num, title = int(m.group(1)), m.group(2).strip()
            elif s and not s.startswith("--"):
                sql_lines.append(line)
        sql = "\n".join(sql_lines).strip()
        if sql:
            blocks.append((num, title or "(ohne Titel)", sql))
    return blocks


def fmt_val(col, val):
    if val is None:
        return "—"
    s = str(val)
    cap = MAXCOL.get(col)
    if cap and len(s) > cap:
        s = s[: cap - 1] + "…"
    return s


def is_num(val):
    return isinstance(val, (int, float))


def print_table(cols, rows):
    if not rows:
        print("   (keine Zeilen)")
        return
    disp = [[fmt_val(c, r[c]) for c in cols] for r in rows]
    widths = [max(len(c), *(len(d[i]) for d in disp)) for i, c in enumerate(cols)]
    numcol = [all(is_num(r[c]) or r[c] is None for r in rows) for c in cols]

    def line(cells):
        return "  ".join(
            (cells[i].rjust(widths[i]) if numcol[i] else cells[i].ljust(widths[i]))
            for i in range(len(cols))
        )

    print("   " + line(list(cols)))
    print("   " + "  ".join("-" * w for w in widths))
    for d in disp:
        print("   " + line(d))


def main():
    only = None
    if "--query" in sys.argv:
        only = int(sys.argv[sys.argv.index("--query") + 1])

    blocks = parse_queries(SQL_FILE)
    conn = pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **DB)

    print(f"\n{'='*84}")
    print(f"  fox2db Entscheidungs-Report   ({len(blocks)} Abschnitte aus {SQL_FILE.name})")
    print(f"{'='*84}")

    with conn.cursor() as cur:
        for num, title, sql in blocks:
            if only is not None and num != only:
                continue
            print(f"\n── {num}) {title} " + "─" * max(0, 80 - len(title)))
            try:
                cur.execute(sql)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                print_table(cols, rows)
                print(f"   → {len(rows)} Zeile(n)")
            except Exception as e:
                print(f"   FEHLER: {e}")
    conn.close()
    print()


if __name__ == "__main__":
    main()
