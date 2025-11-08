WITH exploded AS (
        SELECT
            e.user_pseudo_id,
            e.event_timestamp,
            e.hostname,
            (ep).key AS k,
            (ep).value.string_value AS v
        FROM events AS e,
            UNNEST(e.event_params) AS u(ep)
        ),
        pivoted AS (
        SELECT
            user_pseudo_id,
            MAX(event_timestamp) AS event_timestamp,
            hostname,
            MAX(CASE WHEN k = 'source' THEN v END)   AS source,
            MAX(CASE WHEN k = 'medium' THEN v END)   AS medium,
            MAX(CASE WHEN k = 'campaign' THEN v END) AS campaign
        FROM exploded
        GROUP BY user_pseudo_id, hostname
        ),
        ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp DESC) AS rn
        FROM pivoted
        WHERE source IS NOT NULL
        )
        SELECT user_pseudo_id, hostname, event_timestamp, source, medium, campaign
        FROM ranked
        WHERE rn = 1