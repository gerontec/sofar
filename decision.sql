-- Entscheidungs-Häufigkeitsreport
-- Nutzung: mysql -h 192.168.178.218 -u gh -pa12345 wagodb < decision.sql

SELECT
    d.kategorie,
    d.decision,
    d.erklaerung,
    COUNT(l.id)                                         AS gesamt,
    COUNT(CASE WHEN l.ts >= NOW() - INTERVAL 7 DAY
               THEN 1 END)                             AS letzte_7_tage,
    COUNT(CASE WHEN l.ts >= NOW() - INTERVAL 1 DAY
               THEN 1 END)                             AS letzte_24h,
    MAX(l.ts)                                          AS zuletzt,
    CASE WHEN COUNT(l.id) = 0 THEN '*** NIE ***' ELSE '' END AS hinweis
FROM dim_decisions d
LEFT JOIN pv_decision_log l USING (decision)
GROUP BY d.decision, d.kategorie, d.erklaerung
ORDER BY d.kategorie, gesamt DESC;
