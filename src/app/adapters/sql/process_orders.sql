WITH exploded AS (
        SELECT
            e.event_timestamp,
            e.hostname,
            e.user_pseudo_id,
            e.items,
            (ep).key AS k,
            (ep).value.string_value AS string_val,
            (ep).value.int_value    AS int_val,
            (ep).value.double_value AS double_val
        FROM events AS e,
            UNNEST(e.event_params) AS u(ep)
        WHERE e.event_name = 'purchase'
        ),
        purchases AS (
            SELECT
                event_timestamp,
                hostname,
                user_pseudo_id,
                MAX(CASE WHEN k = 'currency' THEN string_val END) AS currency,
                MAX(CASE WHEN k = 'value' THEN COALESCE(double_val, int_val) END) AS value,
                LIST_TRANSFORM(items, i -> STRUCT_PACK(
                    item_id := i.item_id,
                    item_name := i.item_name,
                    price := i.price,
                    quantity := i.quantity
                )) AS items_simplified
            FROM exploded
            GROUP BY event_timestamp, hostname, user_pseudo_id, items
        ),
        joined AS (
            SELECT
                p.event_timestamp,
                p.hostname,
                p.user_pseudo_id,
                p.currency,
                p.value,
                p.items_simplified,
                a.source,
                a.medium,
                a.campaign
            FROM purchases AS p
            LEFT JOIN purchase_last_click_attributions AS a
                ON p.hostname = a.hostname
            AND p.user_pseudo_id = a.user_pseudo_id
        )
        SELECT *
        FROM joined
        ORDER BY event_timestamp DESC;