WITH filtered_sessions AS (
    SELECT
        session_id,
        user_id,
        campaign,
        utm_source
    FROM
        sessions
    WHERE
        campaign IS NOT NULL
        AND (
            utm_source = 'cpc' OR utm_source = 'cpm' OR utm_source = 'cpa'
            OR utm_source = 'youtube' OR utm_source = 'cpp' OR utm_source = 'tg'
        )
),

last_paid_clicks AS (
    SELECT
        fs.user_id,
        fs.campaign,
        MAX(fs.session_id) AS last_session_id
    FROM
        filtered_sessions AS fs
    GROUP BY
        fs.user_id, fs.campaign
),

clicks_leads AS (
    SELECT
        l.lead_id,
        l.user_id,
        l.created_at,
        lp.last_session_id
    FROM
        leads AS l
    LEFT JOIN
        last_paid_clicks AS lp ON l.user_id = lp.user_id
),

attributed_leads AS (
    SELECT
        cl.lead_id,
        cl.user_id,
        cl.created_at,
        COALESCE(lp.campaign, 'organic') AS campaign
    FROM
        clicks_leads AS cl
    LEFT JOIN
        last_paid_clicks AS lp ON cl.user_id = lp.user_id
)

SELECT
    al.lead_id,
    al.user_id,
    al.created_at,
    al.campaign
FROM
    attributed_leads AS al
ORDER BY
    al.created_at DESC;
