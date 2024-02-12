-- -------------------------------------------------------------------------------------------------
-- Query 3: Local Item Suggestion
-- -------------------------------------------------------------------------------------------------
-- Who is selling in OR, ID or CA in category 10, and for what auction ids?
-- Illustrates an incremental join (using per-key state and timer) and filter.
-- -------------------------------------------------------------------------------------------------

SET 'auto.offset.reset' = 'earliest';

CREATE
OR REPLACE STREAM nexmark_q3 WITH(KAFKA_TOPIC='RESULT_QUERY3', FORMAT='JSON') AS
SELECT P.name,
       P.city,
       P.state,
       A.id,
       A.seller
FROM auctions A
         INNER JOIN people P WITHIN 1 HOURS GRACE PERIOD 15 MINUTES
            ON A.seller = P.id
WHERE A.category = 10
  AND ( P.state = 'OR' OR
        P.state = 'ID' OR
        P.state = 'CA' )
EMIT CHANGES;