#!/usr/bin/env python3
"""
rep_max_state.py - how many decision states of batmonitor can really occur: the reachability computation, printed
step by step with the mathematics behind each step.

It reads the logic batmonitor runs: bm_logic.py, the decision tables bm_decisions.dmn (DMN 1.3) and the site
parameters bm_tables.h of a victron checkout (default ~/victron/venus-addons/batmonitor). Nothing is copied: the
transition function below calls the real decision functions of bm_logic (bm_d1, bm_d2, bm_d3, bm_d7, chain_split).

The model
---------
batmonitor is a finite automaton on its persistent decision state x (the latches that survive a 5 s cycle):

    x(k+1) = f(x(k), u(k))          x in S (finite), u in U (the measured inputs of one cycle)

S is split into components that do not influence each other:

    B = (prot, force, low, chg_prev, prop_prev, split_all) in {0,1}^6      SoC latches, D2 mode, D3, split
    A = bat1_first in {0,1}                                                  D1 Sofar-first latch
    C = rot in {L1, L2, L3}                                                  the rotating single MultiPlus
    D = house_on in {0,1}                                                    D7 house on AC-out1

A couples into B only through the S3 source (prop_hold of D3); that is possible with either value of A, so it
is a free input of B and the reachable set of S is the Cartesian product of the components' reachable sets.

Steps (each printed with its numbers)
1. consistent inputs: every column of a DMN table is a comparison of one measured quantity with a threshold
   (soc < SOC_MIN). A bit vector is feasible iff the system A.x <= b of its comparisons has a solution (LP
   feasibility, Farkas' lemma). With one quantity per comparison, A has one column per quantity and feasibility
   reduces to sorting the thresholds: n thresholds cut the line into at most 2n+1 cells (the thresholds themselves
   matter for <= / >=), and one representative per cell gives every feasible bit vector.
2. transition matrix T (|B| x |B|, boolean): T[i, j] = 1 iff some consistent input u leads from state i to j.
3. reachability matrix R = (I + T)^n, boolean. (I + T)^k holds the states reachable in at most k steps; since a
   path never needs more than |B| - 1 steps, the powers become constant. Repeated squaring reaches the fixpoint
   after at most ceil(log2 |B|) squarings. Reachable from the power-on state s0: row s0 of R.
4. components and product: |reach(S)| = |reach(A)| x |reach(B)| x |reach(C)| x |reach(D)|.
5. structure: strongly connected components of the reachable graph (states that reach each other); one single
   component means no state is a trap, every state leads back to s0 (R[j, s0] = 1 for all reachable j).

usage: python3 rep_max_state.py [--bm DIR] [--set NAME=value ...] [--matrix]
  --set SOC_FORCE=0   explore another parameter value (as the editor would set it), e.g. 66 instead of 78 states
  --matrix            print the reachable part of T as a 0/1 matrix
"""
import argparse
import itertools
import os
import sys

import numpy as np

DEFAULT_BM = os.path.expanduser("~/victron/venus-addons/batmonitor")
EPS = 1e-3
B_NAMES = ("prot", "force", "low", "chg_prev", "prop_prev", "split_all")


def load_logic(bm_dir):
    sys.path.insert(0, bm_dir)
    import bm_logic
    return bm_logic


def site_cfg(L, overrides):
    """the parameters batmonitor runs with: default < PARAM line of bm_tables.h < --set"""
    c = L.Cfg()
    for i, p in enumerate(L.BM_PARAMS):
        if p.name in L.SITE_PARAMS:
            c.set_param(i, L.SITE_PARAMS[p.name])
    for name, v in overrides.items():
        i = next((k for k, p in enumerate(L.BM_PARAMS) if p.name == name), None)
        if i is None or c.set_param(i, v) != 0:
            raise SystemExit("--set %s=%s: unknown parameter or outside its range" % (name, v))
    return c


# ---- step 1: thresholds and consistent inputs --------------------------------------------------------------

def thresholds(L, cfg):
    """{quantity: sorted thresholds} from the DMN inputExpressions, linear in one quantity with coefficient +-1;
    plus the comparisons that are not (they would need a real LP)"""
    import ast
    env = L._expr_env(cfg)
    th, other = {}, []

    def lin(node):
        if isinstance(node, ast.Constant):
            return {}, float(node.value)
        if isinstance(node, ast.Name):
            if node.id in L.DMN_INPUTS:
                return {node.id: 1.0}, 0.0
            return ({}, float(env[node.id])) if node.id in env else None
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            r = lin(node.operand)
            return ({k: -v for k, v in r[0].items()}, -r[1]) if r else None
        if isinstance(node, ast.BinOp):
            a, b = lin(node.left), lin(node.right)
            if a is None or b is None:
                return None
            sg = 1.0 if isinstance(node.op, ast.Add) else -1.0
            co = dict(a[0])
            for k, v in b[0].items():
                co[k] = co.get(k, 0.0) + sg * v
            return co, a[1] + sg * b[1]
        return None

    for cols in L.COLUMNS.values():
        for col, ex in cols:
            if ex is None:
                continue
            for node in ast.walk(ex[0]):
                if not isinstance(node, ast.Compare):
                    continue
                a, b = lin(node.left), lin(node.comparators[0])
                co = None if a is None or b is None else \
                    {k: v for k in set(a[0]) | set(b[0]) for v in [a[0].get(k, 0.0) - b[0].get(k, 0.0)] if v}
                if co and len(co) == 1 and abs(next(iter(co.values()))) == 1.0:
                    k, v = next(iter(co.items()))
                    th.setdefault(k, set()).add(-(a[1] - b[1]) / v)
                else:
                    other.append((col, ast.unparse(node)))
    return {k: sorted(v) for k, v in th.items()}, other


def reps(ths, lo, hi):
    """one representative per cell of the line cut by the thresholds: t - eps, t, t + eps, plus the domain ends"""
    v = {lo, hi}
    for t in ths:
        v.update(x for x in (t - EPS, t, t + EPS) if lo <= x <= hi)
    return sorted(v)


def consistent_inputs(L, cfg, th):
    bank = set()
    for bms_ok, cell_ok, lim_ok, power_ok in itertools.product((0, 1), repeat=4):
        for soc in reps(th["soc"], 0.0, 100.0):
            for cell in reps(th["cell_min"], 2.5, 3.65):
                for dcl in reps(th["dcl"], 0.0, 600.0):
                    for pw in reps(th["power"], -5000.0, 5000.0):
                        b = L.d2_bank_bits(cfg, bms_ok, soc, power_ok, pw, lim_ok, dcl, cell_ok, cell)
                        bank.add(tuple(sorted(b.items())))
    surplus = {tuple(L.table_bits(cfg, ("D2_CHARGE_MODE",), surplus=s)[k] for k in ("surplus_gt_on", "surplus_gt_hold"))
               for s in reps(th["surplus"], -30000.0, 30000.0)}
    split_w = reps([cfg.single_max_w - cfg.single_hyst_w, cfg.single_max_w], 0.0, 12000.0)
    return bank, surplus, split_w


# ---- steps 2 and 3: transition and reachability matrix -----------------------------------------------------

def closure(T):
    """R = (I + T)^n boolean, by repeated squaring until the fixpoint; returns (R, number of squarings)"""
    R = np.eye(len(T), dtype=bool) | T
    k = 0
    while True:
        R2 = (R.astype(np.uint16) @ R.astype(np.uint16)) > 0
        k += 1
        if (R2 == R).all():
            return R, k
        R = R2


def component_b(L, cfg, bank, surplus, split_w):
    states = list(itertools.product((0, 1), repeat=len(B_NAMES)))
    idx = {s: i for i, s in enumerate(states)}
    T = np.zeros((len(states), len(states)), dtype=bool)
    modes = {s: set() for s in states}
    n_inputs = 0
    for s in states:
        prot, force, low, chg_prev, prop_prev, split_all = s
        for bb in bank:
            bk = dict(bb)
            for (on, hold), stale, block, ff, house_import, deficit, prop_hold in itertools.product(
                    surplus, (0, 1), (0, 1), (0, 1), (0, 1), (0, 1), (0, 1)):
                d = {"stale": stale, "block": block, "surplus_gt_on": on, "surplus_gt_hold": hold,
                     "bank": [dict(bk, ff=ff)]}
                p2, f2, l2, _, _, mode = L.bm_d2(d, [prot], [force], [low], chg_prev)
                mode = mode[0]
                n_chg, n_dis = mode == L.BM_M_CHARGE, mode == L.BM_M_DIS
                prop = L.bm_d3(1, house_import, deficit, prop_hold, prop_prev) if n_dis else 0
                for w in (split_w if (n_chg or n_dis) else [0.0]):
                    st = L.State()
                    st.split_all = split_all
                    if n_chg or n_dis:
                        L.chain_split(cfg, st, L.Out(), w)
                    else:
                        st.split_all = 0
                    nxt = (p2[0], f2[0], l2[0], int(n_chg), int(prop), st.split_all)
                    T[idx[s], idx[nxt]] = True
                    modes[s].add(mode)
                    n_inputs += 1
    return states, idx, T, modes, n_inputs


def small_component(transitions, n):
    T = np.zeros((n, n), dtype=bool)
    for i, j in transitions:
        T[i, j] = True
    R, _ = closure(T)
    return int(R[0].sum())


def sccs(R, nodes):
    """strongly connected components from R: i ~ j iff R[i, j] and R[j, i]"""
    left, out = list(nodes), []
    while left:
        i = left[0]
        comp = [j for j in left if R[i, j] and R[j, i]]
        out.append(comp)
        left = [j for j in left if j not in comp]
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--bm", default=DEFAULT_BM, help="batmonitor directory (bm_logic.py, bm_decisions.dmn, bm_tables.h)")
    ap.add_argument("--set", action="append", default=[], metavar="NAME=value", help="parameter override")
    ap.add_argument("--matrix", action="store_true", help="print the reachable part of T")
    a = ap.parse_args()
    overrides = {}
    for kv in a.set:
        k, _, v = kv.partition("=")
        overrides[k.strip()] = float(v)

    L = load_logic(a.bm)
    cfg = site_cfg(L, overrides)
    print("batmonitor logic from %s (%d DMN tables, %d rules)%s" % (
        a.bm, len(L.TABLES), sum(len(r) for _, r in L.TABLES.values()),
        "; overrides " + " ".join("%s=%g" % kv for kv in overrides.items()) if overrides else ""))

    # 1
    th, nonlin = thresholds(L, cfg)
    print("\n1. consistent inputs  -  feasibility of A.x <= b")
    print("   every DMN column compares one quantity with a threshold; per quantity the thresholds cut the line into cells:")
    for q in sorted(th):
        n = len(th[q])
        print("     %-12s thresholds %-34s -> at most %d cells" % (q, ", ".join("%g" % t for t in th[q]), 2 * n + 1))
    print("   comparisons in more than one quantity (would need an LP): %s" % (", ".join("%s: %s" % c for c in nonlin) or "none"))
    bank, surplus, split_w = consistent_inputs(L, cfg, th)
    nb = len(L.COLUMNS["D2_PROT"]) and len({k for t in ("D2_PROT", "D2_FORCE", "D2_LOW", "D2_MODE")
                                             for k, ex in L.COLUMNS[t] if ex is not None}) + 1
    print("   bank columns: %d bits -> 2^%d = %d bit vectors, feasible: %d" % (nb, nb, 2 ** nb, len(bank)))
    print("   surplus columns: 2 bits -> 4 bit vectors, feasible: %d (surplus > 200 W implies surplus > 20 W)" % len(surplus))

    # 2 + 3
    states, idx, T, modes, n_in = component_b(L, cfg, bank, surplus, split_w)
    print("\n2. transition matrix T of component B = %s" % " x ".join("{0,1}" for _ in B_NAMES))
    print("   |B| = 2^%d = %d candidate states; T[i, j] = 1 iff some consistent input leads i -> j" % (len(B_NAMES), len(states)))
    print("   %d (state, input) pairs evaluated, T has %d ones of %d entries" % (n_in, int(T.sum()), T.size))
    R, k = closure(T)
    s0 = idx[(0,) * len(B_NAMES)]
    reach = [j for j in range(len(states)) if R[s0, j]]
    print("\n3. reachability matrix R = (I + T)^n, boolean")
    print("   fixpoint after %d squarings ((I+T)^%d = (I+T)^%d), bound ceil(log2 %d) = %d"
          % (k, 2 ** k, 2 ** (k - 1), len(states), int(np.ceil(np.log2(len(states))))))
    print("   row s0 (power-on state, all 0) of R: %d of %d states reachable:" % (len(reach), len(states)))
    for j in reach:
        s = states[j]
        print("     " + " ".join("%s=%d" % (n, v) for n, v in zip(B_NAMES, s)))
    never = [n for i, n in enumerate(B_NAMES) if not any(states[j][i] for j in reach)]
    print("   bits that never become 1: %s" % (", ".join(never) or "none"))
    if a.matrix:
        sub = T[np.ix_(reach, reach)].astype(int)
        print("   T restricted to the reachable states (rows = from, columns = to, order as listed):")
        for row in sub:
            print("     " + " ".join(str(x) for x in row))

    # 4
    tr_a = []
    for bf in (0, 1):
        for have in (0, 1):
            for soc_b1 in reps(th["soc_bat1"], 0.0, 100.0):
                for night, wp_on, winter, b1_dis in itertools.product((0, 1), repeat=4):
                    b = dict(L.table_bits(cfg, ("D1_LATCH",), have_soc_bat1=have, soc_bat1=soc_b1),
                             night=night, wp_on=wp_on, winter=winter, b1_dis=b1_dis)
                    tr_a.append((bf, L.bm_d1(b, bf)["bat1_first"]))
    n_a = small_component(tr_a, 2)
    mode = L.lround(cfg.house_acout)
    tr_d = []
    for on in (0, 1):
        for total in reps(th["acout_total"], -5000.0, 15000.0):
            for under_long in (0, 1):
                hb = L.table_bits(cfg, ("D7",), acout_total=total)
                nxt, _ = L.bm_d7(mode == 0, mode == 1, hb["over_on"], hb["under_off"], under_long and hb["under_off"], on)
                tr_d.append((on, nxt))
    n_d = small_component(tr_d, 2)
    n_c = small_component([(r, r) for r in range(L.NPH)] + [(r, (r + 1) % L.NPH) for r in range(L.NPH)], L.NPH)
    total = n_a * len(reach) * n_c * n_d
    print("\n4. components and product  |reach(S)| = |A| x |B| x |C| x |D|")
    print("   A bat1_first %d of 2 · B %d of %d · C rot %d of %d · D house_on %d of 2 (HOUSE_ACOUT = %d)"
          % (n_a, len(reach), len(states), n_c, L.NPH, n_d, mode))
    print("   reachable persistent states: %d x %d x %d x %d = %d   (of %d candidates = 2 x 64 x 3 x 2)"
          % (n_a, len(reach), n_c, n_d, total, 2 * len(states) * L.NPH * 2))
    cells = sum(len(modes[states[j]]) for j in reach)
    print("   with the mode chosen in the cycle that leaves a state: %d (state B, mode) cells" % cells)

    # 5
    comps = sccs(R, reach)
    back = all(R[j, s0] for j in reach)
    print("\n5. structure of the reachable graph")
    print("   strongly connected components (i ~ j iff R[i, j] = R[j, i] = 1): %d, sizes %s"
          % (len(comps), sorted((len(c) for c in comps), reverse=True)))
    print("   every reachable state leads back to s0 (column s0 of R all 1): %s -> %s"
          % ("yes" if back else "no", "no trap state" if back else "TRAP states exist"))
    both = [states[j] for j in reach if states[j][3] and states[j][4]]
    print("   chg_prev and prop_prev at once (charging and proportional discharging): %s"
          % ("never" if not both else "%d states!" % len(both)))
    return 0 if back and not both else 1


if __name__ == "__main__":
    sys.exit(main())
