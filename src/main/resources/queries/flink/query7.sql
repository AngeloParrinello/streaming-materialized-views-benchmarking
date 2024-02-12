-- -------------------------------------------------------------------------------------------------
-- Query 7: Highest Bid
-- -------------------------------------------------------------------------------------------------
-- What are the highest bids per period?
--
-- The original Nexmark Query7 calculate the highest bids in the last minute.
-- We will use a shorter window (10 seconds) to help make testing easier.


CREATE TABLE nexmark_q7
(
    auction   BIGINT,
    bidder    BIGINT,
    price     BIGINT,
    entryTime TIMESTAMP(3),
    extra     VARCHAR
) WITH (
      'connector' = 'blackhole'
      );

INSERT INTO nexmark_q7
SELECT B.auction, B.price, B.bidder, B.entryTime, B.extra
from bids B
         JOIN (
    SELECT MAX(price) AS maxprice, CAST(window_end AS TIMESTAMP_LTZ(3)) AS dateTime
    FROM TABLE(TUMBLE(TABLE bids, DESCRIPTOR(entryTime), INTERVAL '10' SECOND))
    GROUP BY window_start, window_end
) B1
              ON B.price = B1.maxprice
WHERE B.entryTime BETWEEN B1.dateTime - INTERVAL '10' SECOND  AND B1.dateTime;

-- This second version it's more compact and efficient, but some tests fail due to
-- a concurrent error during the windowing.
/*INSERT INTO nexmark_q7
SELECT auction, bidder, price, entryTime, extra
FROM bids
WHERE price = (SELECT MAX(price) AS maxprice
               FROM TABLE(TUMBLE(TABLE bids, DESCRIPTOR(entryTime), INTERVAL '10' SECOND))
               GROUP BY window_start, window_end);*/