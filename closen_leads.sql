

WITH lead_sessions AS (
    -- первый переход по платной рекламе
    SELECT 
        s.visitor_id,
        MIN(s.visit_date::date) AS first_visit_date
    FROM 
        sessions s
    WHERE 
        s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    GROUP BY 
        s.visitor_id
),
lead_closing_data AS (
    SELECT 
        l.lead_id,
        ls.first_visit_date::date as first_visit_date,
        l.created_at::date AS lead_creation_date,
        (l.created_at::date - ls.first_visit_date) AS days_to_close
    FROM 
        leads l
    INNER JOIN 
        lead_sessions ls ON l.visitor_id = ls.visitor_id AND l.created_at > ls.first_visit_date 
    WHERE 
        (l.status_id = 142 OR l.closing_reason = 'Успешно реализовано')
),
lead_closing_summary AS (
    SELECT 
        days_to_close,
        COUNT(lead_id) AS leads_count
    FROM 
        lead_closing_data
    GROUP BY 
        days_to_close
),
lead_cumulative AS (
    -- накопительное количество закрытых лидов
    SELECT 
        days_to_close,
        SUM(leads_count) OVER (ORDER BY days_to_close) AS leads,
        SUM(SUM(leads_count)) OVER () AS total_leads
    FROM 
        lead_closing_summary
    GROUP BY days_to_close, leads_count
)
SELECT 
    days_to_close,
    leads,
    total_leads,
    (leads::float / total_leads::float) * 100 AS cumulative_percentage
FROM 
    lead_cumulative
WHERE 
    (leads::float / total_leads::float) >= 0.9
ORDER BY 
    days_to_close
LIMIT 1;
