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

-- 3) STABILIZING hält State obwohl Bat1 stark entlädt (nur in detail, nicht mehr primäre decision)
SELECT ts, state_from, state_to, detail,
       bat1_w, excess_w, soc,
       dc_pv_w, dc_expected_w
FROM pv_decision_log
WHERE detail LIKE '%STABILIZING%'
  AND bat1_w < -500
ORDER BY ts DESC LIMIT 50;

-- 4) PEAK_GATE: Tagesverlauf — wann öffnet/schließt das Gate
SELECT
    DATE(ts)            AS tag,
    MIN(CASE WHEN detail NOT LIKE '%PEAK_GATE%' AND ts > '00:00' THEN ts END) AS gate_offen_ab,
    COUNT(CASE WHEN detail LIKE '%PEAK_GATE%' THEN 1 END)                     AS zyklen_geblockt,
    COUNT(CASE WHEN detail NOT LIKE '%PEAK_GATE%' THEN 1 END)                 AS zyklen_geladen,
    MAX(dc_expected_w)                                                         AS max_dc_expected_w
FROM pv_decision_log
GROUP BY DATE(ts)
ORDER BY tag DESC LIMIT 14;

-- 5) PEAK_FORCE_7 / DO4: Peak-Ereignisse
SELECT ts, state_from, state_to,
       pcc_w, soc, bat1_w, detail
FROM pv_decision_log
WHERE detail LIKE '%PEAK_FORCE_7%'
   OR detail LIKE '%DO4%'
ORDER BY ts DESC LIMIT 50;

-- 6) Hoher Netzbezug trotz Budget-Limit
SELECT ts, state_from, state_to, decision,
       pcc_w, bat1_w, excess_w,
       dc_pv_w, dc_expected_w
FROM pv_decision_log
WHERE pcc_w < -800
ORDER BY ts DESC LIMIT 50;

-- 7) Übersicht verdächtige Fälle nach Typ (letzte 30 Tage)
SELECT
    d.kategorie,
    l.decision,
    d.erklaerung,
    COUNT(*)        AS verdächtige_fälle,
    AVG(l.bat1_w)   AS avg_bat1_w,
    MIN(l.bat1_w)   AS min_bat1_w,
    MAX(l.pcc_w)    AS max_pcc_w
FROM pv_decision_log l
JOIN dim_decisions d USING (decision)
WHERE l.ts >= NOW() - INTERVAL 30 DAY
  AND (l.bat1_w < -500 OR l.pcc_w > 15000 OR l.pcc_w < -800)
GROUP BY l.decision, d.kategorie, d.erklaerung
ORDER BY verdächtige_fälle DESC;

-- 9) Tägliche Häufigkeit je Entscheidung — Ausreisser erkennen (letzte 14 Tage)
SELECT
    DATE(l.ts)                                          AS tag,
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
GROUP BY DATE(l.ts), l.decision, d.kategorie
ORDER BY tag DESC, abweichung DESC;

-- 10) Bewölkungs-Abweichungsanalyse pro Entscheidung
SELECT
    d.kategorie,
    l.decision,
    COUNT(*)                                                          AS cnt,
    ROUND(AVG(l.dc_pv_w))                                            AS avg_dc_pv_w,
    ROUND(AVG(l.dc_expected_w))                                      AS avg_dc_expected_w,
    ROUND(AVG(1 - l.dc_pv_w / NULLIF(l.dc_expected_w, 0)) * 100, 1) AS avg_clouds_pct
FROM pv_decision_log l
JOIN dim_decisions d USING (decision)
WHERE l.dc_expected_w > 500
GROUP BY l.decision, d.kategorie
ORDER BY d.kategorie, avg_clouds_pct DESC;
