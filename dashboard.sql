--4
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
t AS (
    SELECT 
        cast(f.visit_date as DATE) AS visit_date,
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
    SUM(total_cost) AS total_cost,
    SUM(revenue) AS revenue,
    SUM(leads_count) AS leads_count,
    SUM(t.purchases_count) AS purchase_count,
    COALESCE(
        SUM(leads_count) / NULLIF(SUM(t.visitors_count), 0) * 100, 0
    ) AS conversion_r_click_lead,
    COALESCE(
        SUM(t.purchases_count) / NULLIF(SUM(leads_count), 0) * 100, 0
    ) AS conversion_r_lead_purchase,
    COALESCE(SUM(t.total_cost) / NULLIF(SUM(t.visitors_count), 0), 0) AS cpu,
    COALESCE(SUM(t.total_cost) / NULLIF(SUM(t.leads_count), 0), 0) AS cpl,
    COALESCE(SUM(t.total_cost) / NULLIF(SUM(t.purchases_count), 0), 0) AS cppu,
    COALESCE(
        (SUM(t.revenue) - SUM(t.total_cost))
        / NULLIF(SUM(t.total_cost), 0)
        * 100,
        0
    ) AS roi
FROM
    t
GROUP BY
    t.visit_date,
    t.utm_source,
    t.utm_medium,
    t.utm_campaign
ORDER BY
    revenue DESC;

-- 90% лидов
WITH lead_sessions AS (
    SELECT
        s.visitor_id,
        MIN(s.visit_date::date) AS first_visit_date
    FROM
        sessions AS s
    WHERE
        s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    GROUP BY
        s.visitor_id
),

lead_closing_data AS (
    SELECT
        l.lead_id,
        ls.first_visit_date::date AS first_visit_date,
        l.created_at::date AS lead_creation_date,
        (l.created_at::date - ls.first_visit_date) AS days_to_close
    FROM
        leads AS l
    INNER JOIN
        lead_sessions AS ls
        ON l.visitor_id = ls.visitor_id AND l.created_at > ls.first_visit_date
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


-- корреляция органики
WITH daily_visits AS (
    SELECT
        visit_date::date AS visit_date,
        SUM(CASE WHEN medium = 'organic' THEN 1 ELSE 0 END) AS organic_visits
    FROM
        sessions
    GROUP BY
        visit_date::date
),

active_campaigns AS (
    SELECT
        ad.campaign_date::date AS visit_date,
        COUNT(
            DISTINCT CONCAT(ad.utm_source, ad.utm_medium, ad.utm_campaign)
        ) AS active_campaigns
    FROM (
        SELECT
            utm_source,
            utm_medium,
            utm_campaign,
            campaign_date::date AS campaign_date,
            SUM(daily_spent) AS total_cost
        FROM (
            SELECT
                utm_source,
                utm_medium,
                utm_campaign,
                campaign_date,
                daily_spent
            FROM
                vk_ads
            UNION ALL
            SELECT
                utm_source,
                utm_medium,
                utm_campaign,
                campaign_date,
                daily_spent
            FROM
                ya_ads
        ) AS all_ads
        GROUP BY
            utm_source, utm_medium, utm_campaign, campaign_date::date
    ) AS ad
    WHERE ad.total_cost > 0
    GROUP BY
        ad.campaign_date::date
)
--
--SELECT 
--    corr(COALESCE(v.organic_visits, 0), COALESCE(a.active_campaigns, 0)) AS correlation_coefficient
--FROM 
--    daily_visits v
--LEFT JOIN 
--    active_campaigns a ON v.visit_date = a.visit_date;

SELECT
    v.visit_date,
    COALESCE(a.active_campaigns, 0) AS active_campaigns,
    COALESCE(v.organic_visits, 0) AS organic_visits
FROM
    daily_visits AS v
LEFT JOIN
    active_campaigns AS a ON v.visit_date = a.visit_date
ORDER BY
    v.visit_date ASC;
