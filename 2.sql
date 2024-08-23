--Расчет расходов
--Построим витрину со следующими полями:
--visit_date — дата визита
--utm_source / utm_medium / utm_campaign — метки пользователя
--visitors_count — количество визитов в этот день с этими метками
--total_cost — затраты на рекламу
--leads_count — количество лидов, которые оставили визиты, кликнувшие в этот день с этими метками
--purchases_count — количество успешно закрытых лидов (closing_reason = “Успешно реализовано” или status_code = 142)
--revenue — деньги с успешно закрытых лидов

WITH last_paid_sessions AS (
    SELECT 
        s.visitor_id,
        s.visit_date,
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
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(DISTINCT visitor_id) AS visitors_count
    FROM 
        last_paid_sessions
    WHERE 
        rn = 1
    GROUP BY 
        visit_date, utm_source, utm_medium, utm_campaign
),
lead_info AS (
    SELECT 
        s.visit_date,
        s.utm_source,
        s.utm_medium,
        s.utm_campaign,
        SUM(l.amount) AS revenue,
        COUNT(DISTINCT l.lead_id) AS leads_count,
        COUNT(DISTINCT CASE WHEN l.status_id = 142 OR l.closing_reason = 'Успешно реализовано' THEN l.lead_id END) AS purchases_count
    FROM 
        last_paid_sessions s
    LEFT JOIN 
        leads l ON s.visitor_id = l.visitor_id AND l.created_at >= s.visit_date
    WHERE 
        s.rn = 1
    GROUP BY 
        s.visit_date, s.utm_source, s.utm_medium, s.utm_campaign
),
ad_costs AS (
    SELECT 
        va.utm_source,
        va.utm_medium,
        va.utm_campaign,
        va.campaign_date AS visit_date,
        SUM(va.daily_spent) AS total_cost
    FROM 
        vk_ads va
    GROUP BY 
        va.utm_source, va.utm_medium, va.utm_campaign, va.campaign_date
    UNION ALL
    SELECT 
        ya.utm_source,
        ya.utm_medium,
        ya.utm_campaign,
        ya.campaign_date AS visit_date,
        SUM(ya.daily_spent) AS total_cost
    FROM 
        ya_ads ya
    GROUP BY 
        ya.utm_source, ya.utm_medium, ya.utm_campaign, ya.campaign_date
)
SELECT 
	to_char(f.visit_date, 'YYYY-MM') as visit_date,
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
limit 15;               

   
