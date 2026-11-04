#!/usr/bin/python3
# batmonitor rule matrix per day: how many distinct states the system was in.
#
# Only states that dim_bm_matrix_bit explains are reported: minutes inside the validity (dim_bm_matrix
# valid_from .. valid_to) of a matrix version that has rows in dim_bm_matrix_bit (since 2026-11-04 only the
# current one), with a bm_d1_id whose set bits all have a meaning there. Older minutes, minutes without
# bm_d1_id and cells with an unused bit set are skipped and only counted at the end.
# Gaps: every minute of a version's validity (up to now - LOGGER_LAG_MIN) is expected once; a minute without a
# pv_victron row is "no row" (the logger or Venus was down), a row without a decodable state "no state"
# (batmonitor not running, e.g. a restart). Per day as columns, at the end as a list of time ranges.
#
# Per day:
#   exp       minutes expected (version validity, evaluation window, up to now)
#   min       minutes reported (with a decodable state)
#   no row    expected minutes without a pv_victron row
#   no st     minutes with a row but without a decodable state
#   cells     distinct D1 cells (pv_victron.bm_d1_id) seen / cells of that matrix (make check), coverage %
#   d1 chg    changes of the D1 cell from one minute to the next
#   rules     distinct rule states = the triple of the three phase rule texts (bm_l1..l3_rule), with the
#             measured value in FORCE(..%) / PROT(..%) / CHARGING(..W) / LADESPERRE(..h) removed; WPMAX(W)
#             stays (0 / 1000 are two different rules)
#   rule chg  changes of that triple from one minute to the next
# --cells lists every D1 cell per day: its integer value, the bitmask as a sum of powers of 2 (e.g. 94 =
# 2+4+8+16+64), the 8 bits (bit 7 left), its minutes and the set bits (names from dim_bm_matrix_bit).
#
# rep_rulematrix.py [--days 7] [--cells]
# Needs pymysql.

import argparse
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta

import pymysql

DB = dict(host='192.168.178.218', user='gh', password='a12345', database='wagodb')
LOGGER_LAG_MIN = 2                              # the newest minutes may not be written yet
MEASURED = re.compile(r'\b(FORCE|PROT|CHARGING|LADESPERRE)\([^)]*\)')


def load(since):
    """the matrix versions of dim_bm_matrix_bit with their bit names, and the pv_victron minutes inside them"""
    con = pymysql.connect(**DB, cursorclass=pymysql.cursors.DictCursor, connect_timeout=5)
    with con.cursor() as c:
        c.execute("""
            SELECT m.matrix_version, m.git_commit, m.valid_from, m.valid_to, m.d1_cells, m.d1_outcomes
            FROM dim_bm_matrix m
            WHERE EXISTS (SELECT 1 FROM dim_bm_matrix_bit b WHERE b.matrix_version = m.matrix_version)
            ORDER BY m.valid_from""")
        versions = c.fetchall()
        c.execute("SELECT matrix_version, bit_no, bitmask, meaning FROM dim_bm_matrix_bit")
        bits = defaultdict(dict)                # version -> bitmask -> meaning (NULL = unused bit)
        for b in c.fetchall():
            bits[b['matrix_version']][b['bitmask']] = b['meaning']
        rows = []
        for v in versions:
            c.execute("""
                SELECT ts, bm_d1_id d1, bm_l1_rule r1, bm_l2_rule r2, bm_l3_rule r3
                FROM pv_victron
                WHERE ts >= GREATEST(%s, %s) AND (%s IS NULL OR ts < %s)
                ORDER BY ts""", (since, v['valid_from'], v['valid_to'], v['valid_to']))
            for r in c.fetchall():
                r['v'] = v
                rows.append(r)
        c.execute("SELECT COUNT(*) n FROM pv_victron WHERE ts >= %s AND bm_l1_rule IS NOT NULL", (since,))
        total = c.fetchone()['n']
    con.close()
    for v in versions:
        v['bits'] = bits[v['matrix_version']]
    return versions, rows, total


def explained(r):
    """the D1 cell of this minute is decoded by dim_bm_matrix_bit: bm_d1_id there and every set bit has a meaning"""
    d1, names = r['d1'], r['v']['bits']
    return d1 is not None and all(names.get(1 << i) for i in range(8) if d1 >> i & 1)


def has_state(r):
    return all(r[k] is not None for k in ('r1', 'r2', 'r3')) and explained(r)


def minutes(versions, rows, since, end):
    """every expected minute -> (version, row or None): validity of each version inside since .. end"""
    by_ts = {r['ts']: r for r in rows}
    out = []
    for v in versions:
        t = max(since, v['valid_from'])
        if t.second or t.microsecond:
            t = t.replace(second=0, microsecond=0) + timedelta(minutes=1)
        stop = min(end, v['valid_to']) if v['valid_to'] else end
        while t <= stop and (not v['valid_to'] or t < v['valid_to']):
            out.append((t, v, by_ts.get(t)))
            t += timedelta(minutes=1)
    return out


def gap_ranges(mins):
    """runs of consecutive minutes without a state: (first, last, minutes, kind)"""
    runs, cur = [], None
    for t, v, r in mins:
        kind = None if r is not None and has_state(r) else 'no row' if r is None else 'no state'
        if cur and kind == cur[3] and t - cur[1] == timedelta(minutes=1):
            cur[1], cur[2] = t, cur[2] + 1
            continue
        if cur:
            runs.append(tuple(cur))
        cur = [t, t, 1, kind] if kind else None
    if cur:
        runs.append(tuple(cur))
    return runs


def rule_state(r):
    return tuple(MEASURED.sub(r'\1', r[k] or '') for k in ('r1', 'r2', 'r3'))


def powers_of_2(d1):
    return '+'.join(str(1 << i) for i in range(8) if d1 >> i & 1) or '0'


def bits_text(d1, v):
    return ' + '.join(v['bits'][1 << i] for i in range(8) if d1 >> i & 1) or '(none)'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=float, default=7, help='evaluation window in days')
    ap.add_argument('--cells', action='store_true', help='list the D1 cells per day')
    a = ap.parse_args()
    since = (datetime.now() - timedelta(days=a.days)).replace(hour=0, minute=0, second=0, microsecond=0)
    versions, rows, total = load(since)
    end = datetime.now().replace(second=0, microsecond=0) - timedelta(minutes=LOGGER_LAG_MIN)
    mins = minutes(versions, rows, since, end)
    kept = [r for r in rows if has_state(r)]

    groups = defaultdict(list)                  # (day, version) -> rows with a state, in time order
    expect = Counter()                          # (day, version) -> expected / no row / no state minutes
    no_row, no_state = Counter(), Counter()
    vinfo = {}
    for t, v, r in mins:
        key = (t.date(), v['matrix_version'])
        vinfo[key[1]] = v
        expect[key] += 1
        if r is None:
            no_row[key] += 1
        elif not has_state(r):
            no_state[key] += 1
        else:
            groups[key].append(r)

    print('%-10s %-7s %5s %5s %6s %5s %13s %7s %6s %8s  %s' % (
        'day', 'matrix', 'exp', 'min', 'no row', 'no st', 'cells', 'd1 chg', 'rules', 'rule chg',
        'top rule state (minutes)'))
    for (day, ver) in sorted(expect):
        v = vinfo[ver]
        g = groups[(day, ver)]
        head = '%-10s %-7s %5d %5d %6d %5d' % (day, ver, expect[(day, ver)], len(g), no_row[(day, ver)],
                                              no_state[(day, ver)])
        if not g:
            print(head + '  (no state this day)')
            continue
        d1 = [r['d1'] for r in g]
        cells = Counter(d1)
        d1_chg = sum(1 for x, y in zip(g, g[1:]) if x['d1'] != y['d1'])
        states = [rule_state(r) for r in g]
        sc = Counter(states)
        rule_chg = sum(1 for x, y in zip(states, states[1:]) if x != y)
        if cells:
            cell_txt = '%3d/%-3d %3.0f%%' % (len(cells), v['d1_cells'], 100.0 * len(cells) / v['d1_cells'])
        else:
            cell_txt = '-'
        top, n = sc.most_common(1)[0]
        print('%s %13s %7s %6d %8d  %s (%d)' % (head, cell_txt, d1_chg, len(sc), rule_chg, ' | '.join(top), n))
        if a.cells and cells:
            print('%11s %5s  %-20s %-8s %5s  %s' % ('', 'state', 'bitmask', 'bits', 'min', 'set bits'))
            for c, m in sorted(cells.items(), key=lambda x: -x[1]):
                print('%11s %5d  %-20s %08d %5d  %s' % ('', c, powers_of_2(c), int(bin(c)[2:]), m, bits_text(c, v)))

    print('\nmatrix versions (dim_bm_matrix_bit):')
    for v in versions:
        print('  %-7s %s  %s .. %s  %d cells, %d outcomes' % (
            v['matrix_version'], v['git_commit'], v['valid_from'], v['valid_to'] or 'now', v['d1_cells'],
            v['d1_outcomes']))
        for m in sorted(v['bits']):
            print('    bit %d (%3d)  %s' % (m.bit_length() - 1, m, v['bits'][m] or '(unused)'))
    runs = gap_ranges(mins)
    print('\ngaps (minutes without a decodable state, %s .. %s):' % (mins[0][0] if mins else since, end))
    if not runs:
        print('  none')
    for first, last, n, kind in runs:
        print('  %s .. %s  %4d min  %s' % (first, last.strftime('%H:%M') if last.date() == first.date() else last,
                                            n, kind))
    print('  total: %d of %d expected minutes (%d no row, %d no state), coverage %.2f %%' % (
        sum(r[2] for r in runs), len(mins), sum(no_row.values()), sum(no_state.values()),
        100.0 * (len(mins) - sum(r[2] for r in runs)) / len(mins) if mins else 0))
    in_ver = len([r for r in rows if r['r1'] is not None])
    print('\nminutes since %s: %d with a rule, %d reported; skipped: %d outside these versions, %d without '
          'bm_d1_id or with an unused bit' % (since, total, len(kept), total - in_ver, in_ver - len(kept)))


if __name__ == '__main__':
    main()
