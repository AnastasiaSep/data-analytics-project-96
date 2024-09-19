WITH ad_costs AS (
    SELECT 
        utm_source,
        utm_medium,
        utm_campaign,
        CAST(campaign_date AS DATE) AS visit_date,  
        SUM(daily_spent) AS total_cost
    FROM 
        vk_ads
    GROUP BY 
        utm_source, utm_medium, utm_campaign, CAST(campaign_date AS DATE)
    UNION ALL
    SELECT 
        utm_source,
        utm_medium,
        utm_campaign,
        CAST(campaign_date AS DATE) AS visit_date,
        SUM(daily_spent) AS total_cost
    FROM 
        ya_ads
    GROUP BY 
        utm_source, utm_medium, utm_campaign, CAST(campaign_date AS DATE)
),
daily_visits AS (
    SELECT 
        visit_date::date AS visit_date,
        SUM(CASE WHEN source = 'organic' THEN 1 ELSE 0 END) AS organic_visits,
        SUM(CASE WHEN medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social') THEN 1 ELSE 0 END) AS paid_visits
    FROM 
        sessions
    GROUP BY 
        visit_date::date
),
stats AS (
    SELECT 
        AVG(organic_visits) AS avg_organic,
        AVG(COALESCE(a.total_cost, 0)) AS avg_cost
    FROM 
        daily_visits d
    LEFT JOIN 
        ad_costs a ON d.visit_date = a.visit_date
),
correlation_data AS (
    SELECT 
        d.visit_date,
        COALESCE(a.total_cost, 0) AS total_cost,
        d.organic_visits,
        (d.organic_visits - s.avg_organic) * (COALESCE(a.total_cost, 0) - s.avg_cost) AS xy,
        (d.organic_visits - s.avg_organic) ^ 2 AS x2,
        (COALESCE(a.total_cost, 0) - s.avg_cost) ^ 2 AS y2
    FROM 
        daily_visits d
    LEFT JOIN 
        ad_costs a ON d.visit_date = a.visit_date
    CROSS JOIN 
        stats s
)
SELECT 
    SUM(xy) / NULLIF(SQRT(SUM(x2) * SUM(y2)), 0) AS correlation,
    AVG(total_cost) AS avg_ad_cost,
    AVG(organic_visits) AS avg_organic_visits
FROM 
    correlation_data;
