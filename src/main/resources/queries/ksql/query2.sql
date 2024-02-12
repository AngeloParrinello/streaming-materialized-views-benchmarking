-- -------------------------------------------------------------------------------------------------
-- Query2: Selection
-- -------------------------------------------------------------------------------------------------
-- Find bids with specific auction ids and show their bid price.
-- -------------------------------------------------------------------------------------------------

SET 'auto.offset.reset' = 'earliest';

CREATE
OR REPLACE STREAM nexmark_q2 WITH(KAFKA_TOPIC='RESULT_QUERY2', FORMAT='JSON')AS
SELECT auction,
       price
FROM bids
WHERE auction = 1007
   OR auction = 1020
   OR auction = 2001
   OR auction = 2019
   OR auction = 2087
EMIT CHANGES;