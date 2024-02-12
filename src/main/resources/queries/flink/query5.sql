-- -------------------------------------------------------------------------------------------------
-- Query 5: Hot Items
-- -------------------------------------------------------------------------------------------------
-- Which auctions have seen the most bids in the last period?
-- Illustrates sliding windows and combiners.
--
-- The original Nexmark Query5 calculate the hot items in the last hour (updated every minute).
-- To make things a bit more dynamic and easier to test we use much shorter windows,
-- i.e. in the last 10 seconds and update every 2 seconds.
-- -------------------------------------------------------------------------------------------------
-- Subquery 1 (AuctionBids): It calculates the number of bids for each auction within a specific time window.
-- This subquery uses the HOP function to create time-based windows of 10 seconds sliding every 2 seconds.
-- It counts the number of bids in each window for each auction.
-- -------------------------------------------------------------------------------------------------
-- Subquery 2 (MaxBids): This subquery calculates the maximum number of bids within the same time windows as
-- in Subquery 1. It groups the results by the same time window and calculates the maximum bid count within that window.
-- -------------------------------------------------------------------------------------------------
-- Main Query: The main query joins the results of Subquery 1 and Subquery 2 based on the time window and the bid count.
-- It selects auction IDs and the number of bids from the AuctionBids subquery where the bid count is greater
-- than or equal to the maximum bid count calculated in the MaxBids subquery.

CREATE TABLE nexmark_q5
(
    auction BIGINT,
    num     BIGINT
) WITH (
      'connector' = 'blackhole'
      );

INSERT INTO nexmark_q5
SELECT AuctionBids.auction, AuctionBids.num
FROM (
         -- Subquery 1: Count the number of bids for each auction within a time window
         SELECT auction,
                count(*)     AS num,
                window_start AS starttime,
                window_end   AS endtime
         FROM TABLE(
                 HOP(TABLE bids, DESCRIPTOR(entryTime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
         GROUP BY auction, window_start, window_end) AS AuctionBids
         JOIN (
    -- Subquery 2: Find the maximum number of bids within a time window
    SELECT max(CountBids.num) AS maxn,
           CountBids.starttime,
           CountBids.endtime
    FROM (
             -- Subquery 3: Count the number of bids for each auction within a time window
             SELECT count(*)     AS num,
                    window_start AS starttime,
                    window_end   AS endtime
             FROM TABLE(
                     HOP(TABLE bids, DESCRIPTOR(entryTime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
             GROUP BY auction, window_start, window_end) AS CountBids
    GROUP BY CountBids.starttime, CountBids.endtime) AS MaxBids
-- Join the results of the two subqueries based on time window and bid count
              ON AuctionBids.starttime = MaxBids.starttime AND
                 AuctionBids.endtime = MaxBids.endtime AND
                 AuctionBids.num >= MaxBids.maxn;
