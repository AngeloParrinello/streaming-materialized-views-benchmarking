-- Count the number of bids for each auction within a time window
CREATE VIEW
    input_recent_hopping_bucketed_bids_query5
AS
SELECT auction, hopping_window_start, hopping_window_start + (INTERVAL '10 seconds') AS hopping_window_end, count(*) as num
FROM (SELECT *, tunble_window_start - generate_series * '2 seconds'::interval AS hopping_window_start
      FROM (SELECT  (data -> 'actualEvent' -> 'actualEvent' ->>'auction')::bigint as auction,
                    (data -> 'actualEvent' -> 'actualEvent' ->>'bidder')::bigint as bidder,
                    (data -> 'actualEvent' -> 'actualEvent' ->>'entryTime')::timestamp as entryTime,
                    date_bin('10 seconds', (data -> 'actualEvent' -> 'actualEvent' ->>'entryTime')::timestamp, '2000-01-01 00:00:00+00') AS tunble_window_start
            FROM bids_source) times, generate_series(0, ceil(10::decimal/2::decimal)::int - 1)
     )
WHERE entryTime >= hopping_window_start AND entryTime < hopping_window_start + (INTERVAL '10 seconds')
GROUP BY auction, hopping_window_start, hopping_window_end;

CREATE
MATERIALIZED VIEW
    nexmark_q5 AS
SELECT AuctionBids.auction, AuctionBids.num, AuctionBids.hopping_window_start as starttime, AuctionBids.hopping_window_end as endtime
FROM input_recent_hopping_bucketed_bids_query5 AS AuctionBids
JOIN (
    SELECT max(CountBids.num) AS maxn,
           CountBids.hopping_window_start AS starttime,
           CountBids.hopping_window_end AS endtime
    FROM input_recent_hopping_bucketed_bids_query5 AS CountBids
    GROUP BY CountBids.hopping_window_start, CountBids.hopping_window_end) AS MaxBids
ON AuctionBids.hopping_window_start = MaxBids.starttime AND
   AuctionBids.hopping_window_end = MaxBids.endtime AND
   AuctionBids.num >= MaxBids.maxn;