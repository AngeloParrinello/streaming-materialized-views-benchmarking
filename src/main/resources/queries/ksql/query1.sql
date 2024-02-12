-- -------------------------------------------------------------------------------------------------
-- Query1: Currency conversion
-- -------------------------------------------------------------------------------------------------
-- Convert each bid value from dollars to euros. Illustrates a simple transformation.
-- -------------------------------------------------------------------------------------------------

SET 'auto.offset.reset' = 'earliest';

CREATE
OR REPLACE STREAM nexmark_q1 WITH(KAFKA_TOPIC='RESULT_QUERY1', FORMAT='JSON') AS
SELECT auction       AS auction,
       bidder        AS bidder,
       price * 0.908 AS price, -- convert dollar to euro
       entryTime     AS entryTime,
       extra         AS extra
FROM bids
EMIT CHANGES;
