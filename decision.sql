-- Entscheidungs-Häufigkeitsreport
-- Nutzung: mysql -h 192.168.178.218 -u gh -pa12345 wagodb < decision.sql

-- 1) Alle bekannten Entscheidungen mit Häufigkeit (dim_ als Basis, nie = NULL)
SELECT
    d.kategorie,
    d.decision,
    d.erklaerung,
    COUNT(l.id)                                                  AS gesamt,
    COUNT(CASE WHEN l.ts >= NOW() - INTERVAL 7 DAY THEN 1 END)  AS letzte_7_tage,
    COUNT(CASE WHEN l.ts >= NOW() - INTERVAL 1 DAY THEN 1 END)  AS letzte_24h,
    MAX(l.ts)                                                    AS zuletzt,
    CASE WHEN COUNT(l.id) = 0 THEN '*** NIE ***' ELSE '' END     AS hinweis
FROM dim_decisions d
LEFT JOIN pv_decision_log l USING (decision)
GROUP BY d.decision, d.kategorie, d.erklaerung
ORDER BY d.kategorie, gesamt DESC;

-- 2) Undokumentierte Entscheidungen (in Log aber nicht in dim_)
SELECT
    l.decision,
    COUNT(*)  AS cnt,
    MAX(l.ts) AS zuletzt,
    '*** NICHT IN DIM ***' AS hinweis
FROM pv_decision_log l
LEFT JOIN dim_decisions d USING (decision)
WHERE d.decision IS NULL
GROUP BY l.decision
ORDER BY cnt DESC;

-- 3) Fehlentscheidung: STABILIZING hält State → BAT1 entlädt stark
SELECT ts, state_from, state_to, detail,
       bat1_w, excess_w, soc,
       dc_pv_w, dc_expected_w,
       ROUND((1 - dc_pv_w / NULLIF(dc_expected_w, 0)) * 100, 1) AS clouds_pct
FROM pv_decision_log
WHERE decision = 'STABILIZING'
  AND bat1_w < -500
ORDER BY ts DESC LIMIT 50;

-- 4) Fehlentscheidung: CLOUD_FREE freigegeben aber PCC sehr hoch (Cap-Risiko)
SELECT ts, state_from, state_to, detail,
       pcc_w, excess_w, soc,
       dc_pv_w, dc_expected_w,
       ROUND((1 - dc_pv_w / NULLIF(dc_expected_w, 0)) * 100, 1) AS clouds_pct
FROM pv_decision_log
WHERE decision = 'CLOUD_FREE'
  AND pcc_w > 15000
ORDER BY ts DESC LIMIT 50;

-- 5) Fehlentscheidung: FeedInLimiter SKIP aber PCC nahe Limit (Risiko unterschätzt)
SELECT ts, state_from, state_to, detail,
       pcc_w, excess_w, soc,
       dc_pv_w, dc_expected_w,
       ROUND((1 - dc_pv_w / NULLIF(dc_expected_w, 0)) * 100, 1) AS clouds_pct
FROM pv_decision_log
WHERE decision = 'FeedInLimiter SKIP'
  AND pcc_w > 14000
ORDER BY ts DESC LIMIT 50;

-- 6) Fehlentscheidung: hoher Netzbezug trotz Budget-Limit
SELECT ts, state_from, state_to, decision,
       pcc_w, bat1_w, excess_w,
       dc_pv_w, dc_expected_w,
       ROUND((1 - dc_pv_w / NULLIF(dc_expected_w, 0)) * 100, 1) AS clouds_pct
FROM pv_decision_log
WHERE pcc_w < -800
ORDER BY ts DESC LIMIT 50;

-- 7) Übersicht Fehlentscheidungen nach Typ (letzte 30 Tage)
SELECT
    d.kategorie,
    l.decision,
    d.erklaerung,
    COUNT(*)                                           AS verdächtige_fälle,
    AVG(l.bat1_w)                                     AS avg_bat1_w,
    MIN(l.bat1_w)                                     AS min_bat1_w,
    MAX(l.pcc_w)                                      AS max_pcc_w,
    ROUND(AVG(1 - l.dc_pv_w / NULLIF(l.dc_expected_w, 0)) * 100, 1) AS avg_clouds_pct
FROM pv_decision_log l
JOIN dim_decisions d USING (decision)
WHERE l.ts >= NOW() - INTERVAL 30 DAY
  AND (l.bat1_w < -500 OR l.pcc_w > 15000 OR l.pcc_w < -800)
GROUP BY l.decision, d.kategorie, d.erklaerung
ORDER BY verdächtige_fälle DESC;

-- 8) Bewölkungs-Abweichungsanalyse: Messung vs. Klarhimmel pro Entscheidung
SELECT
    d.kategorie,
    l.decision,
    COUNT(*)                                                          AS cnt,
    ROUND(AVG(l.dc_pv_w))                                            AS avg_dc_pv_w,
    ROUND(AVG(l.dc_expected_w))                                      AS avg_dc_expected_w,
    ROUND(AVG(1 - l.dc_pv_w / NULLIF(l.dc_expected_w, 0)) * 100, 1) AS avg_clouds_pct,
    ROUND(MAX(1 - l.dc_pv_w / NULLIF(l.dc_expected_w, 0)) * 100, 1) AS max_clouds_pct
FROM pv_decision_log l
JOIN dim_decisions d USING (decision)
WHERE l.dc_expected_w > 500
GROUP BY l.decision, d.kategorie
ORDER BY d.kategorie, avg_clouds_pct DESC;
