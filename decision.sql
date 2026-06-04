-- Entscheidungs-Report fox2db v2.8
-- Nutzung: mysql -h 192.168.178.218 -u gh -pa12345 wagodb < decision.sql
--
-- Architektur:
--   prio 0       = LADESPERRE_BIS_PCC_20KW (Hard Guard, greift nach decide())
--   prio 1       = PCC_OVER_20KW (Peak-Begrenzung, in decide())
--   prio 2       = EMERGENCY_FORCE (Blocking-Bypass, in detail sichtbar)
--   prio 3..8    = Primary Decisions (decide()) — in decision-Spalte
--   prio 7..8    = HARD_GUARDS (BATTERY_FULL_STOP, CRITICAL_SOC) — in detail als "| GUARD:X"
--   prio 9       = POWER_MATCHING + RAMP_LIMITED — Default, in decision-Spalte / detail
--   prio 11..15  = Blocking Rules (SWEET_SPOT_HOLD..HYSTERESIS) — in detail sichtbar

-- ─────────────────────────────────────────────────────────────────────────────
-- 1) Alle bekannten Entscheidungen mit Häufigkeit (dim_ als Basis, nie = NULL)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    d.prio,
    d.kategorie,
    d.decision,
    d.erklaerung,
    COUNT(l.id)                                                   AS gesamt,
    COUNT(CASE WHEN l.ts >= NOW() - INTERVAL 7 DAY  THEN 1 END)  AS letzte_7_tage,
    COUNT(CASE WHEN l.ts >= NOW() - INTERVAL 1 DAY  THEN 1 END)  AS letzte_24h,
    MAX(l.ts)                                                     AS zuletzt,
    CASE WHEN COUNT(l.id) = 0 THEN '*** NIE ***' ELSE '' END      AS hinweis
FROM dim_decisions d
LEFT JOIN pv_decision_log l ON (
    l.decision = d.decision
    OR (d.decision = 'LADESPERRE_BIS_PCC_20KW'          AND l.detail LIKE '%GUARD:LADESPERRE_BIS_PCC_20KW%')
    OR (d.decision = 'EMERGENCY_FORCE'                   AND l.detail LIKE '%EMERGENCY_FORCE%')
    OR (d.decision = 'PCC_OVER_20KW'                     AND l.detail LIKE '%PCC>20kW%')
    OR (d.decision = 'BATTERY_FULL_STOP'                 AND l.detail LIKE '%GUARD:BATTERY_FULL_STOP%')
    OR (d.decision = 'CRITICAL_SOC_PROTECTION_ACTIVATE'  AND l.detail LIKE '%GUARD:CRITICAL_SOC%')
    OR (d.decision = 'RAMP_LIMITED'                      AND l.detail LIKE '%RAMP_LIMITED%')
    OR (d.decision = 'SWEET_SPOT_HOLD'                   AND l.detail LIKE '%SWEET_SPOT_HOLD%')
    OR (d.decision = 'TREND_BLOCK'                       AND l.detail LIKE '%TREND_BLOCK%')
    OR (d.decision = 'BAT_GUARD_BLOCK'                   AND l.detail LIKE '%BAT_GUARD_BLOCK%')
    OR (d.decision = 'STABILIZING'                       AND l.detail LIKE '%STABILIZING%')
    OR (d.decision = 'HYSTERESIS'                        AND l.detail LIKE '%HYSTERESIS%')
)
GROUP BY d.prio, d.decision, d.kategorie, d.erklaerung
ORDER BY d.prio;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2) Undokumentierte Entscheidungen (in Log aber nicht in dim_)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    l.decision,
    COUNT(*)   AS cnt,
    MAX(l.ts)  AS zuletzt,
    '*** NICHT IN DIM ***' AS hinweis
FROM pv_decision_log l
LEFT JOIN dim_decisions d USING (decision)
WHERE d.decision IS NULL
GROUP BY l.decision
ORDER BY cnt DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3) HARD_GUARDS: wann haben Guards eingegriffen?
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    ts, state_from, state_to, decision, detail, soc, pcc_w
FROM pv_decision_log
WHERE detail LIKE '%GUARD:%'
ORDER BY ts DESC LIMIT 50;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4) EMERGENCY_FORCE: Netzbezug-Ereignisse
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    ts, state_from, state_to, pcc_w, bat1_w, soc, excess_w, detail
FROM pv_decision_log
WHERE detail LIKE '%EMERGENCY_FORCE%'
ORDER BY ts DESC LIMIT 50;

-- ─────────────────────────────────────────────────────────────────────────────
-- 5) PCC_OVER_20KW + DO4 + Ladesperre: Peak- und Release-Ereignisse
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    ts, action, relay, duration_s, state_new, reason
FROM pv_relay_events
WHERE reason LIKE '%PCC>20kW%'
   OR relay = 'do4'
   OR relay = 'ladesperre'
ORDER BY ts DESC LIMIT 50;

-- ─────────────────────────────────────────────────────────────────────────────
-- 6) Hoher Netzbezug trotz Regelung (pcc_w < -800W)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    ts, state_from, state_to, decision,
    pcc_w, bat1_w, excess_w, ebox_w, dc_expected_w
FROM pv_decision_log
WHERE pcc_w < -800
ORDER BY ts DESC LIMIT 50;

-- ─────────────────────────────────────────────────────────────────────────────
-- 7) Verdächtige Fälle nach Typ — letzte 30 Tage
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    d.prio,
    d.kategorie,
    l.decision,
    COUNT(*)       AS verdächtige_fälle,
    AVG(l.bat1_w)  AS avg_bat1_w,
    MIN(l.bat1_w)  AS min_bat1_w,
    MAX(l.pcc_w)   AS max_pcc_w
FROM pv_decision_log l
JOIN dim_decisions d USING (decision)
WHERE l.ts >= NOW() - INTERVAL 30 DAY
  AND (l.bat1_w < -500 OR l.pcc_w > 15000 OR l.pcc_w < -800)
GROUP BY l.decision, d.prio, d.kategorie
ORDER BY d.prio;

-- ─────────────────────────────────────────────────────────────────────────────
-- 8) Blocking-Analyse: welche Regeln verhindern Zustandswechsel?
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN detail LIKE '%SWEET_SPOT_HOLD%'  THEN 'SWEET_SPOT_HOLD'
        WHEN detail LIKE '%TREND_BLOCK%'      THEN 'TREND_BLOCK'
        WHEN detail LIKE '%BAT_GUARD_BLOCK%'  THEN 'BAT_GUARD_BLOCK'
        WHEN detail LIKE '%STABILIZING%'      THEN 'STABILIZING'
        WHEN detail LIKE '%HYSTERESIS%'       THEN 'HYSTERESIS'
        ELSE 'KEIN_BLOCK'
    END                                                AS blocking_rule,
    COUNT(*)                                           AS anzahl,
    COUNT(CASE WHEN ts >= NOW() - INTERVAL 1 DAY THEN 1 END) AS letzte_24h,
    AVG(pcc_w)                                         AS avg_pcc_w,
    AVG(excess_w)                                      AS avg_excess_w
FROM pv_decision_log
WHERE ts >= NOW() - INTERVAL 7 DAY
GROUP BY blocking_rule
ORDER BY anzahl DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 9) Tägliche Häufigkeit je Entscheidung — Ausreißer (letzte 14 Tage)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    DATE(l.ts)                                          AS tag,
    d.prio,
    d.kategorie,
    l.decision,
    COUNT(*)                                            AS anzahl,
    ROUND(AVG(COUNT(*)) OVER (
        PARTITION BY l.decision
        ORDER BY DATE(l.ts)
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ))                                                  AS avg_7_tage,
    COUNT(*) - ROUND(AVG(COUNT(*)) OVER (
        PARTITION BY l.decision
        ORDER BY DATE(l.ts)
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ))                                                  AS abweichung
FROM pv_decision_log l
JOIN dim_decisions d USING (decision)
WHERE l.ts >= NOW() - INTERVAL 14 DAY
GROUP BY DATE(l.ts), l.decision, d.prio, d.kategorie
ORDER BY tag DESC, d.prio;

-- ─────────────────────────────────────────────────────────────────────────────
-- 10) Bewölkungs-Abweichungsanalyse pro Entscheidung
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    d.prio,
    d.kategorie,
    l.decision,
    COUNT(*)                                                           AS cnt,
    ROUND(AVG(l.ebox_w))                                               AS avg_ebox_w,
    ROUND(AVG(l.dc_expected_w))                                        AS avg_dc_expected_w,
    ROUND(AVG(l.dc_delta_w / NULLIF(l.dc_expected_w, 0)) * 100, 1)   AS avg_clouds_pct
FROM pv_decision_log l
JOIN dim_decisions d USING (decision)
WHERE l.dc_expected_w > 500
GROUP BY l.decision, d.prio, d.kategorie
ORDER BY d.prio;
