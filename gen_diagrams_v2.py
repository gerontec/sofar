#!/usr/bin/env python3
import subprocess
from pathlib import Path

OUT = Path(__file__).parent

PAGE1 = """
digraph Architecture {
    graph [
        label="fox2db.py v2.9-Py — Schichten-Architektur (prozedural, 632 Zeilen)"
        labelloc=t fontsize=14 fontname="Helvetica-Bold"
        rankdir=TB splines=ortho nodesep=0.6 ranksep=0.8
        bgcolor="#f8f9fa" size="11,17" ratio=fill
    ]
    node [fontname="Helvetica" fontsize=10 margin="0.2,0.08" style="filled,rounded"]
    edge [fontname="Helvetica" fontsize=10 penwidth=1.2]

    subgraph cluster_extern {
        label="Externe Systeme" style="dashed,filled" fillcolor="#eeeeee"
        fontname="Helvetica-Bold" fontsize=10 color="#aaaaaa"

        MQTT_IN  [shape=cylinder fillcolor="#dddddd" label="MQTT-Broker\nkellertreppe:1883\ninverter/power_grid_exchange/json"]
        MQTT_Z2  [shape=cylinder fillcolor="#dddddd" label="MQTT-Broker\npv_zaehl2/#\n(Z2-Zähler, retained)"]
        MQTT_PUB [shape=cylinder fillcolor="#dddddd" label="MQTT-Broker\nfox2db/state\n(retain=True)"]
        MariaDB  [shape=cylinder fillcolor="#dddddd" label="MariaDB wagodb\npv_decision_log (version, dc_delta_w)\npv_relay_events"]
        Relay    [shape=component fillcolor="#dddddd" label="ebyte_ctrl.py\nRelais-Hardware\nState 0..7"]
        WR2      [shape=component fillcolor="#fce8e8" label="WR2\n(Wechselrichter 2)\nDO4 → Abregelung"]
        EBox     [shape=component fillcolor="#dddddd" label="ebox1arg.py\n/tmp/ebox15k.txt\nSOC (Ø-Module), Strom"]
    }

    subgraph cluster_input {
        label="INPUT LAYER" style="filled" fillcolor="#d4edda"
        fontname="Helvetica-Bold" fontsize=10 color="#28a745"

        fetch_mqtt [shape=box fillcolor="#b8dfc4" label="fetch_mqtt()\nMQTT empfangen\nNone-Felder → 0"]
        fetch_z2   [shape=box fillcolor="#b8dfc4" label="fetch_z2()\nZ2-Zähler lesen\n(Ersatz bei PCC=NaN)"]
        read_ebox  [shape=box fillcolor="#b8dfc4" label="read_ebox()\nSOC = Ø(Module >0%)\n-1 wenn unbekannt/0%"]
        z2_fb      [shape=diamond fillcolor="#fff3cd" label="PCC = NaN?"]
        pcc_node   [shape=box fillcolor="#b8dfc4" label="pcc = abs(min(0, wirkleist))\n(Z2-Fallback)"]
        ladesperre_chk [shape=box fillcolor="#fce8e8" label="_ladesperre_active()\nDB: kein do4/ladesperre heute?\nSchlechtwetter: PCC-avg<20% von DC-Modell"]
    }

    subgraph cluster_decision {
        label="DECISION LAYER" style="filled" fillcolor="#e2d9f3"
        fontname="Helvetica-Bold" fontsize=10 color="#6f42c1"

        decide_fn [shape=box fillcolor="#c9b8f0"
                   label="decide()  — Dominanzgeordnet (erste Regel gewinnt) —\n\nprio 1  EBOX_SOC_UNKNOWN_HOLD  (soc<0)\nprio 2  PCC_OVER_20KW          (pcc>20kW, soc<100 → State+1)\nprio 3  EMERGENCY_CHARGE_TO_7% (prot + soc<7)\nprio 4  CHARGE_TARGET_REACHED  (prot + soc≥7)\nprio 5  INSUFFICIENT_EXCESS    (excess<1010W)\nprio 6  POWER_MATCHING         (Default)\n         └─ RAMP_LIMITED       (max +1 State/Zyklus)"]

        hard_guards [shape=box fillcolor="#f8d7da"
                     label="apply_guards()  — HARD GUARDS (nach decide, unüberwindbar) —\n\nprio 0  LADESPERRE_BIS_PCC_20KW  (kein DO4 heute → State 0)\nprio 7  BATTERY_FULL_STOP        (soc > 99 → State 0)\nprio 6  CRITICAL_SOC_PROTECTION  (soc <  6 → State 1)"]

        blocking [shape=box fillcolor="#d4c5f0"
                  label="apply_blocking()  — Verhindert zu schnelle Zustandswechsel —\n\nEMERGENCY_FORCE (Bypass bei Import >1020W, Richtung DOWN)\n\nUP-Sperren:   SWEET_SPOT_HOLD / TREND_BLOCK / BAT_GUARD_BLOCK\nDOWN-Sperren: STABILIZING / HYSTERESIS"]

        dc_calc [shape=box fillcolor="#c9b8f0" label="_DcForecast.now()\nMeinel-Modell WR1+WR2\nKlarhimmel-Prognose\ndc_expected, peak_today, win_end"]
    }

    subgraph cluster_output {
        label="OUTPUT LAYER" style="filled" fillcolor="#cce5ff"
        fontname="Helvetica-Bold" fontsize=10 color="#004085"

        log_fn    [shape=box fillcolor="#a8d0f5" label="_log()\nZeit + Version + Meldung"]
        publish   [shape=box fillcolor="#a8d0f5" label="publish_mqtt()\nJSON → fox2db/state\n(inkl. need_downward_regulation, ladesperre)"]
        set_relay [shape=box fillcolor="#a8d0f5" label="set_relay()\nebyte_ctrl.py aufrufen"]
        db_log    [shape=box fillcolor="#a8d0f5" label="_db_decision_log()\n_db_relay_event()\n→ MariaDB (version, dc_delta_w)"]
        do4       [shape=box fillcolor="#f8d7da" label="pulse_do4()\nRelais 4 → WR2 abregeln\nNur wenn PCC>20kW\naber kein State+1 möglich\n(SOC=100 oder State=7)"]
    }

    MQTT_IN  -> fetch_mqtt [color="#28a745"]
    MQTT_Z2  -> fetch_z2   [color="#28a745"]
    EBox     -> read_ebox  [color="#28a745"]
    fetch_mqtt -> z2_fb
    fetch_z2   -> z2_fb
    z2_fb -> pcc_node [label="JA" color="red"]

    fetch_mqtt -> decide_fn [color="#6f42c1" label="pcc, bat1"]
    read_ebox  -> decide_fn [color="#6f42c1" label="bat_cur, soc"]
    ladesperre_chk -> hard_guards [color="#c0392b" label="ladesperre"]
    dc_calc    -> log_fn    [color="#6f42c1" label="dc_expected\ndc_delta"]
    decide_fn  -> hard_guards [color="#6f42c1"]
    hard_guards -> blocking   [color="#6f42c1"]

    blocking -> set_relay [color="#004085"]
    blocking -> publish   [color="#004085"]
    blocking -> db_log    [color="#004085"]
    blocking -> do4       [color="#c0392b" style=dashed]

    set_relay -> Relay    [color="#004085"]
    do4       -> WR2      [color="#c0392b" label="DO4\nRelais 4\n3s-Puls"]
    publish   -> MQTT_PUB [color="#004085"]
    db_log    -> MariaDB  [color="#004085"]
}
"""

PAGE2 = """
digraph MainFlow {
    graph [
        label="fox2db.py v2.9-Py — main() Ablauf"
        labelloc=t fontsize=14 fontname="Helvetica-Bold"
        rankdir=TB splines=polyline nodesep=0.4 ranksep=0.5
        bgcolor="#f8f9fa" size="11,17" ratio=fill
    ]
    node [fontname="Helvetica" fontsize=10 margin="0.18,0.08" style="filled,rounded"]
    edge [fontname="Helvetica" fontsize=10 penwidth=1.2]

    Start  [shape=oval fillcolor="#cce5ff" label="main() Start\n(Cron, jede Minute)"]
    End    [shape=oval fillcolor="#cce5ff" label="Ende"]
    EndErr [shape=oval fillcolor="#f8d7da" label="Ende Emergency"]

    s1  [shape=box fillcolor="#fff3cd" label="_log('--- Start Cycle ---')"]
    s2  [shape=box fillcolor="#d4edda" label="fetch_mqtt()\n→ pcc, bat1, soc_bat1"]
    d1  [shape=diamond fillcolor="#fce8e8" label="mqtt_data\n= None?"]
    s3  [shape=box fillcolor="#f8d7da" label="set_relay(0)\n_log('EMERGENCY SHUTDOWN')"]
    s4  [shape=box fillcolor="#d4edda" label="read_ebox()\n→ bat_cur, soc (Ø-Module)\n   soc=-1 wenn 0% oder unbekannt"]
    s5  [shape=box fillcolor="#d4edda" label="fetch_z2()\n→ wirkleist (Ersatz bei NaN)"]
    d2  [shape=diamond fillcolor="#fff3cd" label="isnan(pcc)?"]
    s5b [shape=box fillcolor="#fff3cd" label="pcc = abs(min(0, wirkleist))\nZ2-Fallback"]
    s6  [shape=box fillcolor="#d4edda" label="Zustand lesen:\nrelay_st, stable, prot\nlast_excess, dc_expected\ndc_delta = dc_expected−(pcc+ebox+bat1)"]

    sp_ls [shape=box fillcolor="#fce8e8"
           label="_ladesperre_active()\nDB: do4 oder ladesperre heute?\n→ ladesperre = True/False"]
    d_ls  [shape=diamond fillcolor="#fce8e8" label="ladesperre\naktiv?"]
    d_sw  [shape=diamond fillcolor="#fce8e8" label="Schlechtwetter?\nPCC-avg(10min) < 20%\nvon DC-Modell"]
    r_sw  [shape=box fillcolor="#fce8e8" label="_ladesperre_release_db()\nladesperre = False"]
    r_log [shape=box fillcolor="#fff3cd" label="_log('LADESPERRE aktiv —\nwarte auf DO4-Peak')"]

    s7  [shape=box fillcolor="#e2d9f3" label="decide(soc, pcc, bat_cur, bat1, relay_st, prot)\n→ best, trace, excess"]
    s8  [shape=box fillcolor="#f8d7da" label="apply_guards(best, soc, ladesperre)\n→ LADESPERRE (→0) / BATTERY_FULL_STOP (→0)\n→ CRITICAL_SOC (→1)"]
    s9  [shape=box fillcolor="#e2d9f3" label="drop_rate = (excess − last_excess) / 30\n_write(last_excess)"]
    s10 [shape=box fillcolor="#e2d9f3" label="apply_blocking(best, relay_st, pcc, bat1,\n               stable, drop_rate)\n→ final, changed, trace"]
    s11 [shape=box fillcolor="#cce5ff" label="_log(Data: SOC/PCC/Bat/EBox/DC_exp/DC_delta)\n_log(Result: State + TRACE)"]
    s12 [shape=box fillcolor="#cce5ff" label="publish_mqtt()\n→ fox2db/state\n(need_downward_regulation, ladesperre)"]
    s13 [shape=box fillcolor="#cce5ff" label="_write(last_change, inverter_csv)"]
    s14 [shape=box fillcolor="#e2d9f3" label="Deep Discharge Hysterese\nsoc<lower → prot=1\nsoc≥upper → prot=0"]
    s15 [shape=box fillcolor="#cce5ff" label="_db_decision_log()\n→ pv_decision_log (version, dc_delta_w)"]
    d3  [shape=diamond fillcolor="#fff3cd" label="changed?"]
    s16a [shape=box fillcolor="#cce5ff" label="_db_relay_event()\nset_relay(final)"]
    s16b [shape=box fillcolor="#d4edda" label="_write(relay_state, final)\n(kein Relay-Wechsel)"]
    d4  [shape=diamond fillcolor="#fce8e8" label="PCC >20kW und\nkein PCC_OVER_20KW\nim trace?"]
    s17 [shape=box fillcolor="#f8d7da" label="pulse_do4()\nRelais 4 → WR2 abregeln\n(SOC=100 → kein State+1 mehr möglich)\n→ Ladesperre wird aufgehoben (1. DO4)"]

    Start -> s1 -> s2 -> d1
    d1 -> s3  [label="JA" color="red"]
    d1 -> s4  [label="NEIN" color="green"]
    s3 -> EndErr
    s4 -> s5 -> d2
    d2 -> s5b [label="JA" color="red"]
    d2 -> s6  [label="NEIN" color="green"]
    s5b -> s6
    s6 -> sp_ls -> d_ls
    d_ls -> d_sw  [label="JA" color="red"]
    d_ls -> s7    [label="NEIN" color="green"]
    d_sw -> r_sw  [label="JA\n(ratio>0.8)" color="orange"]
    d_sw -> r_log [label="NEIN" color="#888888"]
    r_sw -> s7
    r_log -> s7
    s7 -> s8 -> s9 -> s10 -> s11 -> s12 -> s13 -> s14 -> s15 -> d3
    d3 -> s16a [label="JA" color="green"]
    d3 -> s16b [label="NEIN" color="#888888"]
    s16a -> d4
    s16b -> d4
    d4 -> s17 [label="JA" color="red"]
    d4 -> End [label="NEIN" color="green"]
    s17 -> End
}
"""

PAGE3 = """
digraph DecisionDetail {
    graph [
        label="fox2db.py v2.9-Py — Decision Layer im Detail"
        labelloc=t fontsize=14 fontname="Helvetica-Bold"
        rankdir=TB splines=polyline nodesep=0.4 ranksep=0.5
        bgcolor="#f8f9fa" size="11,17" ratio=fill
    ]
    node [fontname="Helvetica" fontsize=10 margin="0.18,0.08" style="filled,rounded"]
    edge [fontname="Helvetica" fontsize=10 penwidth=1.2]

    // ── decide() ────────────────────────────────────────────────────────
    Start [shape=oval fillcolor="#e2d9f3" label="decide()"]
    End   [shape=oval fillcolor="#e2d9f3" label="→ (best, trace, excess)"]

    excess_calc [shape=box fillcolor="#c9b8f0"
                 label="excess = pcc + ebox_eff + bat1\nebox_eff = max(bat_cur×106+1, STATE_POWER[relay_st])"]

    d_soc_unk [shape=diamond fillcolor="#fce8e8" label="soc < 0?\n(unbekannt/0%)\n← prio 1"]
    r_soc_unk [shape=box fillcolor="#fce8e8" label="return relay_st\nEBOX_SOC_UNKNOWN_HOLD"]

    d_pcc20 [shape=diamond fillcolor="#f8d7da" label="pcc >20000W\nund soc <100?\n← prio 2"]
    r_pcc20 [shape=box fillcolor="#f8d7da" label="return min(relay_st+1, 7)\nPCC_OVER_20KW"]

    d_prot  [shape=diamond fillcolor="#fce8e8" label="prot aktiv?\n← prio 3/4"]
    d_prot2 [shape=diamond fillcolor="#fce8e8" label="soc < 7%?"]
    r_emerg [shape=box fillcolor="#fce8e8" label="return State 1\nEMERGENCY_CHARGE_TO_7%"]
    r_target [shape=box fillcolor="#fce8e8" label="return State 0\nCHARGE_TARGET_REACHED"]

    d_excess [shape=diamond fillcolor="#fce8e8" label="excess < 1010W?\n← prio 5"]
    r_insuf  [shape=box fillcolor="#fce8e8" label="return State 0\nINSUFFICIENT_EXCESS"]

    pm     [shape=box fillcolor="#c9b8f0"
            label="POWER_MATCHING  ← prio 6\nbudget = excess + 1500W\nbest = höchster State mit P ≤ budget"]
    d_ramp [shape=diamond fillcolor="#fff3cd" label="best > relay_st\nund best > next_up?"]
    r_ramp [shape=box fillcolor="#fff3cd" label="best = next_state_up(relay_st)\nRAMP_LIMITED"]

    // ── apply_guards() ─────────────────────────────────────────────────
    StartG [shape=oval fillcolor="#f8d7da" label="apply_guards(best, soc, ladesperre)"]
    GEnd   [shape=oval fillcolor="#f8d7da" label="→ (best, trace)"]

    g0  [shape=diamond fillcolor="#fce8e8" label="ladesperre\naktiv?\n← prio 0"]
    g0r [shape=box fillcolor="#f8d7da" label="best = 0\nGUARD:LADESPERRE_BIS_PCC_20KW"]
    g1  [shape=diamond fillcolor="#fce8e8" label="soc > 99?\n← prio 7"]
    g1r [shape=box fillcolor="#f8d7da" label="best = 0\nGUARD:BATTERY_FULL_STOP"]
    g2  [shape=diamond fillcolor="#fce8e8" label="0 ≤ soc < 6?\n← prio 6"]
    g2r [shape=box fillcolor="#f8d7da" label="best = 1\nGUARD:CRITICAL_SOC_PROTECTION"]

    // ── apply_blocking() ───────────────────────────────────────────────
    StartB [shape=oval fillcolor="#d4c5f0" label="apply_blocking()"]
    BEnd   [shape=oval fillcolor="#d4c5f0" label="→ (final, changed, trace)"]

    b_same  [shape=diamond fillcolor="#d4c5f0" label="best == relay_st?"]
    b_same_r [shape=box fillcolor="#d4c5f0" label="return relay_st\nchanged=False"]
    b_dir   [shape=box fillcolor="#d4c5f0" label="direction = UP / DOWN\npwr_diff = |P[best] − P[relay_st]|"]
    b_emf   [shape=diamond fillcolor="#fce8e8" label="pcc < -1020W\nund DOWN?"]
    b_emf_r [shape=box fillcolor="#f8d7da" label="EMERGENCY_FORCE\nreturn best, changed=True"]
    b_rules [shape=box fillcolor="#d4c5f0"
             label="Blocking Rules (in Reihenfolge):\nUP:   SWEET_SPOT_HOLD / TREND_BLOCK / BAT_GUARD_BLOCK\nDOWN: STABILIZING / HYSTERESIS"]
    b_blk   [shape=diamond fillcolor="#d4c5f0" label="Regel greift?"]
    b_yes   [shape=box fillcolor="#d4c5f0" label="return relay_st\nchanged=False | trace+=RULE"]
    b_no    [shape=box fillcolor="#d4c5f0" label="return best\nchanged=True"]

    // ── decide() Flow ───────────────────────────────────────────────────
    Start       -> excess_calc
    excess_calc -> d_soc_unk
    d_soc_unk   -> r_soc_unk [label="JA" color="red"]
    d_soc_unk   -> d_pcc20   [label="NEIN" color="green"]
    r_soc_unk   -> End
    d_pcc20     -> r_pcc20   [label="JA" color="red"]
    d_pcc20     -> d_prot    [label="NEIN" color="green"]
    r_pcc20     -> End
    d_prot      -> d_prot2   [label="JA" color="orange"]
    d_prot      -> d_excess  [label="NEIN" color="green"]
    d_prot2     -> r_emerg   [label="JA" color="red"]
    d_prot2     -> r_target  [label="NEIN" color="green"]
    r_emerg     -> End
    r_target    -> End
    d_excess    -> r_insuf   [label="JA" color="red"]
    d_excess    -> pm        [label="NEIN" color="green"]
    r_insuf     -> End
    pm          -> d_ramp
    d_ramp      -> r_ramp    [label="JA" color="orange"]
    d_ramp      -> End       [label="NEIN" color="green"]
    r_ramp      -> End

    // ── apply_guards() Flow ─────────────────────────────────────────────
    StartG -> g0
    g0  -> g0r  [label="JA" color="red"]
    g0  -> g1   [label="NEIN" color="green"]
    g0r -> GEnd
    g1  -> g1r  [label="JA" color="red"]
    g1  -> g2   [label="NEIN" color="green"]
    g1r -> GEnd
    g2  -> g2r  [label="JA" color="red"]
    g2  -> GEnd [label="NEIN" color="green"]
    g2r -> GEnd

    // ── apply_blocking() Flow ───────────────────────────────────────────
    StartB   -> b_same
    b_same   -> b_same_r [label="JA" color="#888888"]
    b_same   -> b_dir    [label="NEIN" color="green"]
    b_same_r -> BEnd
    b_dir    -> b_emf
    b_emf    -> b_emf_r  [label="JA" color="red"]
    b_emf    -> b_rules  [label="NEIN" color="green"]
    b_emf_r  -> BEnd
    b_rules  -> b_blk
    b_blk    -> b_yes    [label="JA" color="orange"]
    b_blk    -> b_no     [label="NEIN" color="green"]
    b_yes    -> BEnd
    b_no     -> BEnd
}
"""


def render(dot_src: str, out_path: Path) -> Path:
    dot_file = out_path.with_suffix(".dot")
    dot_file.write_text(dot_src)
    result = subprocess.run(
        ["dot", "-Tpdf", str(dot_file), "-o", str(out_path)],
        capture_output=True, text=True
    )
    dot_file.unlink()
    if result.returncode != 0:
        print(f"FEHLER {out_path.name}: {result.stderr[:200]}")
        return None
    print(f"OK  → {out_path}")
    return out_path


def main():
    pages = []
    for i, src in enumerate([PAGE1, PAGE2, PAGE3], 1):
        p = render(src, OUT / f"fox2db_v2_page{i}.pdf")
        if p:
            pages.append(str(p))

    if len(pages) == 3:
        out = OUT / "fox2db_v2_flowchart.pdf"
        result = subprocess.run(["pdfunite"] + pages + [str(out)],
                                capture_output=True, text=True)
        if result.returncode == 0:
            print(f"\nKombiniert → {out}")
            for p in pages:
                Path(p).unlink()
        else:
            print(f"pdfunite Fehler: {result.stderr}")


if __name__ == "__main__":
    main()
