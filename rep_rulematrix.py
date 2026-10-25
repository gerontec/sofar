#!/usr/bin/python3
# batmonitor rule matrix per day: how many distinct states the system was in.
#
# Per day and D1 matrix version (wagodb.dim_bm_matrix, joined on pv_victron.ts):
#   min       minutes with a batmonitor rule
#   cells     distinct D1 cells (pv_victron.bm_d1_id) seen / cells of that matrix (make check), coverage %
#   d1 chg    changes of the D1 cell from one minute to the next
#   rules     distinct rule states = the triple of the three phase rule texts (bm_l1..l3_rule), with the
#             measured value in FORCE(..%) / PROT(..%) / CHARGING(..W) / LADESPERRE(..h) removed; WPMAX(W)
#             stays (0 / 1000 are two different rules)
#   rule chg  changes of that triple from one minute to the next
# Rows before the first matrix version show as "-" (no bm_d1_id, rule texts only); a version whose
# bm_d1_id values were deleted (dim_bm_matrix.rows_kept = 0) has no cells either.
# --cells lists every D1 cell per day: its integer value, the bitmask as a sum of powers of 2 (e.g. 94 =
# 2+4+8+16+64), the 8 bits (bit 7 left), its minutes and the set bits (names from dim_bm_matrix).
#
# rep_rulematrix.py [--days 7] [--cells]
# Needs pymysql.

import argparse
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta

import pymysql

DB = dict(host='192.168.178.218', user='gh', password='a12345', database='wagodb')
MEASURED = re.compile(r'\b(FORCE|PROT|CHARGING|LADESPERRE)\([^)]*\)')


def load(since):
    con = pymysql.connect(**DB, cursorclass=pymysql.cursors.DictCursor, connect_timeout=5)
    with con.cursor() as c:
        c.execute("SELECT * FROM dim_bm_matrix ORDER BY valid_from")
        versions = c.fetchall()
        c.execute("""
            SELECT ts, bm_d1_id d1, bm_l1_rule r1, bm_l2_rule r2, bm_l3_rule r3
            FROM pv_victron WHERE ts >= %s AND bm_l1_rule IS NOT NULL ORDER BY ts""", (since,))
        rows = c.fetchall()
    con.close()
    return versions, rows


def version_of(ts, versions):
    for v in versions:
        if ts >= v['valid_from'] and (v['valid_to'] is None or ts < v['valid_to']):
            return v
    return None


def rule_state(r):
    return tuple(MEASURED.sub(r'\1', r[k] or '') for k in ('r1', 'r2', 'r3'))


def powers_of_2(d1):
    return '+'.join(str(1 << i) for i in range(8) if d1 >> i & 1) or '0'


def bits_text(d1, v):
    names = [v['bit%d' % i] or 'bit%d' % i for i in range(8)]
    return ' + '.join(names[i] for i in range(8) if d1 >> i & 1) or '(none)'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=float, default=7, help='evaluation window in days')
    ap.add_argument('--cells', action='store_true', help='list the D1 cells per day')
    a = ap.parse_args()
    since = (datetime.now() - timedelta(days=a.days)).replace(hour=0, minute=0, second=0, microsecond=0)
    versions, rows = load(since)

    groups = defaultdict(list)                  # (day, version) -> rows in time order
    vinfo = {}
    for r in rows:
        v = version_of(r['ts'], versions)
        key = (r['ts'].date(), v['matrix_version'] if v else '-')
        vinfo[key[1]] = v
        groups[key].append(r)

    print('%-10s %-7s %5s %13s %7s %6s %8s  %s' % ('day', 'matrix', 'min', 'cells', 'd1 chg', 'rules',
                                                   'rule chg', 'top rule state (minutes)'))
    for (day, ver), g in sorted(groups.items()):
        v = vinfo[ver]
        d1 = [r['d1'] for r in g if r['d1'] is not None]
        cells = Counter(d1)
        d1_chg = sum(1 for x, y in zip(g, g[1:]) if x['d1'] is not None and y['d1'] is not None and x['d1'] != y['d1'])
        states = [rule_state(r) for r in g]
        sc = Counter(states)
        rule_chg = sum(1 for x, y in zip(states, states[1:]) if x != y)
        if v and cells:
            cell_txt = '%3d/%-3d %3.0f%%' % (len(cells), v['d1_cells'], 100.0 * len(cells) / v['d1_cells'])
        else:
            cell_txt = '-'
        top, n = sc.most_common(1)[0]
        print('%-10s %-7s %5d %13s %7s %6d %8d  %s (%d)' % (day, ver, len(g), cell_txt, d1_chg if d1 else '-',
                                                          len(sc), rule_chg, ' | '.join(top), n))
        if a.cells and cells:
            print('%11s %5s  %-20s %-8s %5s  %s' % ('', 'state', 'bitmask', 'bits', 'min', 'set bits'))
            for c, m in sorted(cells.items(), key=lambda x: -x[1]):
                print('%11s %5d  %-20s %08d %5d  %s' % ('', c, powers_of_2(c), int(bin(c)[2:]), m, bits_text(c, v)))

    print('\nmatrix versions (dim_bm_matrix):')
    for v in versions:
        print('  %-7s %s  %s .. %s  %d cells, %d outcomes%s' % (
            v['matrix_version'], v['git_commit'], v['valid_from'], v['valid_to'] or 'now', v['d1_cells'],
            v['d1_outcomes'], '' if v['rows_kept'] else '  (bm_d1_id deleted)'))


if __name__ == '__main__':
    main()
