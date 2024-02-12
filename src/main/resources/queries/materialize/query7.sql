/*
 Create a view that filters the input for the most recent 30 days and buckets records into 10 seconds windows.
 The date_bin functions returns the largest value less than or equal to source (in this case the entryTime)
 that is a multiple of stride (10 seconds) starting at origin (2000-01-01 00:00:00+00). The window_end is the
 date_bin + 10 seconds. The window_start is the date_bin. So for instance, if the entryTime is
 2023-09-19 10:19:22, the date_bin function search the most recent date that is a multiple of 10 seconds starting
 from 2000-01-01 00:00:00+00. Since 10:19:22 is closer to 10:19:20 than 10:19:30, the date_bin function returns
 2023-09-19 10:19:20. Finally, the window_start is 2023-09-19 10:19:20 and the window_end is 2023-09-19 10:19:30
 (window_start + 10 seconds).
 -------------------------------------------------------------------------------------------------------
 The interval '30 days' is used to filter the input for the most recent 30 days. The mz_now() function returns
    the current time. The mz_now() function is used to filter the input for the most recent 30 days.
 */
CREATE VIEW
    input_recent_bucketed_bids_query7
    AS
    SELECT
        (B.data -> 'actualEvent' -> 'actualEvent' ->>'auction')::bigint as auction,
        (B.data -> 'actualEvent' -> 'actualEvent' ->>'bidder')::bigint as bidder,
        (B.data -> 'actualEvent' -> 'actualEvent' ->>'price')::bigint as price,
        (B.data -> 'actualEvent' -> 'actualEvent' ->>'entryTime'):: timestamp as entryTime,
        (B.data -> 'actualEvent' -> 'actualEvent' ->>'extra') as extra,
        date_bin(
            '10 seconds',
            (B.data -> 'actualEvent' -> 'actualEvent' ->>'entryTime'):: timestamp,
            '2000-01-01 00:00:00+00'
            ) as window_start,
        date_bin(
            '10 seconds',
            (B.data -> 'actualEvent' -> 'actualEvent' ->>'entryTime'):: timestamp,
            '2000-01-01 00:00:00+00'
            ) + INTERVAL '10 seconds' as window_end
    FROM bids_source as B
    WHERE mz_now() <= (B.data -> 'actualEvent' -> 'actualEvent' ->>'entryTime')::timestamp + INTERVAL '30 days';

/*
 Create the final output view that does the aggregation and maintains 7 days worth of results.
 The most internal WHERE clause means “the result for a 10 seconds window should come into effect when mz_now()
 reaches window_end and be removed 7 days later”. Without the latter constraint, records in the result set
 would receive strange updates as records expire from the initial 30 day filter on the input.
 */
CREATE
MATERIALIZED VIEW IF NOT EXISTS nexmark_q7 AS
SELECT (B.data -> 'actualEvent' -> 'actualEvent' ->>'auction')::bigint as auction,
       (B.data -> 'actualEvent' -> 'actualEvent' ->>'bidder')::bigint as bidder,
       (B.data -> 'actualEvent' -> 'actualEvent' ->>'price')::bigint as price,
       (B.data -> 'actualEvent' -> 'actualEvent' ->>'entryTime'):: timestamp as entryTime,
       (B.data -> 'actualEvent' -> 'actualEvent' ->>'extra') as extra,
       S.window_end as window_end,
       S.maxprice as maxprice,
       S.window_start as window_start
FROM bids_source as B,
    (SELECT MAX(price) AS maxprice, window_start, window_end
     FROM input_recent_bucketed_bids_query7
     WHERE mz_now() >= window_end
       AND mz_now() < window_end + INTERVAL '7 days'
     GROUP BY window_end, window_start) as S
WHERE (B.data -> 'actualEvent' -> 'actualEvent' ->>'price')::bigint = S.maxprice
ORDER BY S.window_end, S.window_start, S.maxprice;

