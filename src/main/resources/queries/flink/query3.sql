-- -------------------------------------------------------------------------------------------------
-- Query 3: Local Item Suggestion
-- -------------------------------------------------------------------------------------------------
-- Who is selling in OR, ID or CA in category 10, and for what auction ids?
-- Illustrates an incremental join (using per-key state and timer) and filter.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q3
(
    name  VARCHAR,
    city  VARCHAR,
    state VARCHAR,
    id    BIGINT
) WITH (
      'connector' = 'blackhole'
      );

INSERT INTO nexmark_q3
SELECT P.name,
       P.city,
       P.state,
       A.id
FROM auctions AS A
         INNER JOIN people AS P on A.seller = P.id
WHERE A.category = 10
  and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA');