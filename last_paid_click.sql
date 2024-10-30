WITH last_paid_sessions AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source,
        s.medium,
        s.campaign,
        ROW_NUMBER() OVER (PARTITION BY s.visitor_id ORDER BY s.visit_date DESC) AS rn
    FROM
        sessions AS s
    WHERE
        s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
)

SELECT
    l.visitor_id,
    s.visit_date,
    s.source AS utm_source,
    s.medium AS utm_medium,
    s.campaign AS utm_campaign,
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id
FROM
    last_paid_sessions AS s
LEFT JOIN
    leads AS l
    ON  s.visitor_id = l.visitor_id
    AND s.visit_date <= l.created_at
WHERE
    s.rn = 1  -- Выбираем последний платный клик для каждого посетителя
ORDER BY
    l.amount DESC NULLS LAST,
    s.visit_date ASC,
    s.source ASC,
    s.medium ASC,
    s.campaign ASC
LIMIT 10;
