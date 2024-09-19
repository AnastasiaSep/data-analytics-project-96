WITH last_paid_sessions AS (
    SELECT 
        s.visitor_id,
        s.visit_date AS visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        ROW_NUMBER() OVER (PARTITION BY s.visitor_id ORDER BY s.visit_date DESC) AS rn
    FROM 
        sessions s
    WHERE 
        s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),
filtered_sessions AS (
    SELECT 
        visit_date::date as visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(DISTINCT visitor_id) AS visitors_count
    FROM 
        last_paid_sessions
    WHERE 
        rn = 1
    GROUP BY 
        visit_date::date, utm_source, utm_medium, utm_campaign
),
lead_info AS (
    SELECT 
        s.visit_date::date as visit_date,  
        s.utm_source,
        s.utm_medium,
        s.utm_campaign,
        SUM(l.amount) AS revenue,
        COUNT(DISTINCT l.lead_id) AS leads_count,
        COUNT(DISTINCT CASE WHEN l.status_id = 142 OR l.closing_reason = 'Успешно реализовано' THEN l.lead_id END) AS purchases_count
    FROM 
        last_paid_sessions s
    LEFT JOIN 
        leads l ON s.visitor_id = l.visitor_id AND l.created_at > s.visit_date 
    WHERE 
        s.rn = 1
    GROUP BY 
        s.visit_date::date, s.utm_source, s.utm_medium, s.utm_campaign
),
ad_costs AS (
    SELECT 
        va.utm_source,
        va.utm_medium,
        va.utm_campaign,
        cast(va.campaign_date as DATE) AS visit_date,  
        SUM(va.daily_spent) AS total_cost
    FROM 
        vk_ads va
    GROUP BY 
        va.utm_source, va.utm_medium, va.utm_campaign, cast(va.campaign_date as DATE)
    UNION ALL
    SELECT 
        ya.utm_source,
        ya.utm_medium,
        ya.utm_campaign,
        cast(ya.campaign_date as DATE) AS visit_date,
        SUM(ya.daily_spent) AS total_cost
    FROM 
        ya_ads ya
    GROUP BY 
        ya.utm_source, ya.utm_medium, ya.utm_campaign, cast(ya.campaign_date as DATE)
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
        AVG(paid_visits) AS avg_paid,
        COUNT(*) AS n
    FROM 
        daily_visits
),
correlation_data AS (
    SELECT 
        (organic_visits - s.avg_organic) * (paid_visits - s.avg_paid) AS xy,
        (organic_visits - s.avg_organic) ^ 2 AS x2,
        (paid_visits - s.avg_paid) ^ 2 AS y2
    FROM 
        daily_visits, stats s
),
t AS (
    SELECT 
        f.visit_date,
        f.utm_source,
        f.utm_medium,
        f.utm_campaign,
        f.visitors_count,
        COALESCE(a.total_cost, 0) AS total_cost,
        COALESCE(l.revenue, 0) AS revenue,
        COALESCE(l.leads_count, 0) AS leads_count,
        COALESCE(l.purchases_count, 0) AS purchases_count 
    FROM 
        filtered_sessions f
    LEFT JOIN 
        lead_info l ON f.visit_date = l.visit_date
        AND f.utm_source = l.utm_source
        AND f.utm_medium = l.utm_medium
        AND f.utm_campaign = l.utm_campaign
    LEFT JOIN 
        ad_costs a ON f.visit_date = a.visit_date
        AND f.utm_source = a.utm_source
        AND f.utm_medium = a.utm_medium
        AND f.utm_campaign = a.utm_campaign
    ORDER BY 
        l.revenue DESC NULLS LAST,    
        f.visit_date ASC,              
        f.visitors_count DESC,         
        f.utm_source,                  
        f.utm_medium,                 
        f.utm_campaign
)
SELECT 
    t.visit_date,
    t.utm_source,
    t.utm_medium,
    t.utm_campaign,
    SUM(t.visitors_count) AS visits_count,
    SUM(t.total_cost) AS total_cost,
    SUM(t.revenue) AS revenue,
    SUM(t.leads_count) AS leads_count,
    SUM(t.purchases_count) AS purchase_count,
    COALESCE(SUM(t.leads_count) / NULLIF(SUM(t.visitors_count), 0) * 100, 0) AS conversion_r_click_lead,
    COALESCE(SUM(t.purchases_count) / NULLIF(SUM(t.leads_count), 0) * 100, 0) AS conversion_r_lead_purchase,
    COALESCE(SUM(t.total_cost) / NULLIF(SUM(t.visitors_count), 0), 0) AS cpu,
    COALESCE(SUM(t.total_cost) / NULLIF(SUM(t.leads_count), 0), 0) AS cpl,
    COALESCE(SUM(t.total_cost) / NULLIF(SUM(t.purchases_count), 0), 0) AS cppu,
    COALESCE((SUM(t.revenue) - SUM(t.total_cost)) / NULLIF(SUM(t.total_cost), 0) * 100, 0) AS roi,
    corr_data.correlation
FROM 
    t
LEFT JOIN 
    (SELECT SUM(xy) / SQRT(SUM(x2) * SUM(y2)) AS correlation FROM correlation_data) corr_data ON TRUE
GROUP BY 
    t.visit_date,
    t.utm_source,
    t.utm_medium,
    t.utm_campaign,
    corr_data.correlation
ORDER BY 
    SUM(t.revenue) DESC; 



