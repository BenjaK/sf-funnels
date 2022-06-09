WITH event_data AS (
SELECT 1 AS user_id, TIMESTAMP('2022-01-01 00:00:00') AS event_at, 'A' AS event_name UNION ALL
SELECT 1 AS user_id, TIMESTAMP('2022-01-01 00:00:01') AS event_at, 'B' AS event_name UNION ALL
SELECT 1 AS user_id, TIMESTAMP('2022-01-01 00:00:02') AS event_at, 'C' AS event_name UNION ALL

SELECT 2 AS user_id, TIMESTAMP('2022-01-01 00:00:00') AS event_at, 'A' AS event_name UNION ALL

SELECT 3 AS user_id, TIMESTAMP('2022-01-01 00:00:00') AS event_at, 'A' AS event_name UNION ALL
SELECT 3 AS user_id, TIMESTAMP('2022-01-01 00:00:01') AS event_at, 'B' AS event_name UNION ALL

SELECT 4 AS user_id, TIMESTAMP('2022-01-01 00:00:01') AS event_at, 'B' AS event_name UNION ALL

SELECT 5 AS user_id, TIMESTAMP('2022-01-01 00:00:02') AS event_at, 'C' AS event_name UNION ALL

SELECT 6 AS user_id, TIMESTAMP('2022-01-01 00:00:00') AS event_at, 'A' AS event_name UNION ALL
SELECT 6 AS user_id, TIMESTAMP('2022-04-01 00:00:00') AS event_at, 'B' AS event_name UNION ALL
SELECT 6 AS user_id, TIMESTAMP('2022-07-01 00:00:00') AS event_at, 'C' AS event_name UNION ALL

SELECT 7 AS user_id, TIMESTAMP('2022-01-01 00:00:00') AS event_at, 'A' AS event_name UNION ALL
SELECT 7 AS user_id, TIMESTAMP('2022-01-01 00:00:01') AS event_at, 'B' AS event_name UNION ALL
SELECT 7 AS user_id, TIMESTAMP('2022-01-01 00:00:02') AS event_at, 'A' AS event_name UNION ALL
SELECT 7 AS user_id, TIMESTAMP('2022-01-01 00:00:03') AS event_at, 'C' AS event_name
)

-- This could be a metadata table/view
, event_enum AS (
SELECT 'A' AS event_name, 1 AS event_ordering UNION ALL
SELECT 'B' AS event_name, 2 AS event_ordering UNION ALL
SELECT 'C' AS event_name, 3 AS event_ordering
)

, collected_events AS (
SELECT 
    user_id,
    ARRAY_AGG(STRUCT(event_name, event_ordering, event_at)) AS events
FROM
    event_data
LEFT JOIN
    event_enum
USING
    (event_name)
GROUP BY
    user_id
)

-- Find transitions from one state/event to the next for each user
, funnels AS (
SELECT
    user_id,
    ARRAY(
        SELECT
            STRUCT(
                a.event_ordering AS parent_event_ordering,
                a.event_name AS parent_event,
                b.event_name AS child_event,
                b.event_name IS NOT NULL AS funneled
            )
        FROM
            UNNEST(events) AS a
        LEFT JOIN
            UNNEST(events) AS b
        ON
            a.event_ordering + 1 = b.event_ordering
            AND b.event_at > a.event_at
            AND b.event_at < TIMESTAMP_ADD(a.event_at, INTERVAL 30 DAY)
    ) AS processed_events
FROM
    collected_events
)

-- Filter out duplicate event counts: maybe some users transitioned from a given state to the next more than once?
, unique_funnels AS (
SELECT
    user_id,
    IF(
        (
            SELECT 
                MIN(parent_event_ordering)
            FROM 
                UNNEST(processed_events)
        ) = 1,
        1,
        NULL
    ) AS has_first_event,
    ARRAY(
        SELECT
            STRUCT(
                child_event,
                IF(LOGICAL_OR(funneled), 1, NULL) AS funneled
            )
        FROM
            UNNEST(processed_events) AS a
        WHERE
            child_event IS NOT NULL
        GROUP BY
            child_event
    ) AS processed_events
FROM
    funnels
)

-- Do an intermediate count of events in order to avoid processing the per-user data twice.
-- Because we end up with two columns of counts, we later need to pivot this data to only
-- have one column of counts.
, grouped_counts AS (
SELECT
    child_event AS event_name,
    SUM(has_first_event) AS has_first_event,
    SUM(funneled) AS funneled_users
FROM
    unique_funnels
LEFT JOIN
    UNNEST(processed_events)
GROUP BY
    1
)

SELECT
    event_name, 
    event_ordering,
    funneled_users
FROM (
    SELECT
        (SELECT event_name FROM event_enum WHERE event_ordering = 1) AS event_name,
        SUM(has_first_event) AS funneled_users
    FROM
        grouped_counts
    WHERE
        event_name IS NOT NULL

    UNION ALL

    SELECT
        event_name,
        SUM(funneled_users) AS funneled_users
    FROM
        grouped_counts
    WHERE
        event_name IS NOT NULL
    GROUP BY
        event_name
)
INNER JOIN
    event_enum
USING 
    (event_name)
ORDER BY 
    event_ordering
