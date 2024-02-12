-- -------------------------------------------------------------------------------------------------
-- Query2: Selection
-- -------------------------------------------------------------------------------------------------
-- Find bids with specific auction ids and show their bid price.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q2
(
    auction BIGINT,
    price   BIGINT
) WITH (
      'connector' = 'blackhole'
      );

INSERT INTO nexmark_q2
SELECT auction, price
FROM bids
WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;