CREATE VIEW
    input_recent_bucketed_auctions_query8
AS
SELECT
    (data -> 'actualEvent' -> 'actualEvent' ->>'seller')::bigint as seller,
        date_bin(
                '10 seconds',
                (data -> 'actualEvent' -> 'actualEvent' ->>'entryTime'):: timestamp,
                '2000-01-01 00:00:00+00'
            ) as window_start,
    date_bin(
            '10 seconds',
            (data -> 'actualEvent' -> 'actualEvent' ->>'entryTime'):: timestamp,
            '2000-01-01 00:00:00+00'
        ) + INTERVAL '10 seconds' as window_end
        FROM auctions_source
        WHERE mz_now() <= (data -> 'actualEvent' -> 'actualEvent' ->>'entryTime')::timestamp + INTERVAL '30 days'
        GROUP BY seller, window_start, window_end;

CREATE VIEW
    input_recent_bucketed_people_query8
AS
SELECT
    (data -> 'actualEvent' -> 'actualEvent' ->>'id')::bigint as id,
        (data -> 'actualEvent' -> 'actualEvent' ->>'name') as name,
    date_bin(
            '10 seconds',
            (data -> 'actualEvent' -> 'actualEvent' ->>'entryTime'):: timestamp,
            '2000-01-01 00:00:00+00'
        ) as window_start,
    date_bin(
            '10 seconds',
            (data -> 'actualEvent' -> 'actualEvent' ->>'entryTime'):: timestamp,
            '2000-01-01 00:00:00+00'
        ) + INTERVAL '10 seconds' as window_end
        FROM people_source
        WHERE mz_now() <= (data -> 'actualEvent' -> 'actualEvent' ->>'entryTime')::timestamp + INTERVAL '30 days'
        GROUP BY id, name, window_start, window_end;

CREATE
MATERIALIZED VIEW IF NOT EXISTS nexmark_q8 AS
SELECT P.id, P.name, P.window_start, P.window_end
FROM input_recent_bucketed_people_query8 AS P JOIN input_recent_bucketed_auctions_query8 AS A
ON P.id = A.seller AND P.window_start = A.window_start AND P.window_end = A.window_end;

