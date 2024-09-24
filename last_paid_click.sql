--построим витрину для модели атрибуции Last Paid Click. Витрина должна содержать следующие данные
--visitor_id — уникальный человек на сайте
--visit_date — время визита
--utm_source / utm_medium / utm_campaign — метки c учетом модели атрибуции
--lead_id — идентификатор лида, если пользователь сконвертился в лид после(во время) визита, NULL — если пользователь не оставил лид
--created_at — время создания лида, NULL — если пользователь не оставил лид
--amount — сумма лида (в деньгах), NULL — если пользователь не оставил лид
--closing_reason — причина закрытия, NULL — если пользователь не оставил лид
--status_id — код причины закрытия, NULL — если пользователь не оставил лид
--Клик считается платным для следующих рекламных компаний:
--cpc  cpm   cpa   youtube   cpp   tg   social


WITH last_paid_sessions AS (
    SELECT 
        s.visitor_id,
        s.visit_date,
        s.source,
        s.medium,
        s.campaign,
        ROW_NUMBER() OVER (PARTITION BY s.visitor_id ORDER BY s.visit_date DESC) AS rn
    FROM 
        sessions s

    WHERE 
        s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'))

SELECT 
    l.visitor_id,
    l.lead_id,
    l.amount,
    l.created_at,
    l.closing_reason,
    l.status_id,
    s.visit_date,
    s.source AS utm_source,
    s.medium AS utm_medium,
    s.campaign AS utm_campaign
FROM 
    last_paid_sessions s
LEFT JOIN leads l ON s.visitor_id = l.visitor_id and 
    l.created_at >= s.visit_date 
WHERE 
    s.rn = 1  -- Выбираем последний платный клик для каждого посетителя
ORDER BY 
    l.amount DESC NULLS LAST, 
    s.visit_date ASC, 
    s.source ASC, 
    s.medium ASC, 
    s.campaign asc
limit 10;
   
 