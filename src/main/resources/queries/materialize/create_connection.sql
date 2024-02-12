CREATE
CONNECTION IF NOT EXISTS kafka_connection TO KAFKA (BROKER 'broker:9092');

/*
For Materialize Cloud:
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'b-1-public.nexmarktest.p67uqm.c8.kafka.eu-central-1.amazonaws.com:9196',
    SASL MECHANISMS = 'SCRAM-SHA-512',
    SASL USERNAME = 'angelo.parrinello@agilelab.it',
    SASL PASSWORD = SECRET msk_password
  )

Remember that the MSK cluster MUST be public available, the security group must allow the connection from the Materialize Cloud cluster.
Moreover, the cluster must have enabled the SASL/SCRAM authentication mechanism, you need to have a symmetric key
and a secret associated to the symmetric key (the secret is the password) and associated to the cluster.
 */

CREATE
SOURCE IF NOT EXISTS bids_source
FROM KAFKA CONNECTION kafka_connection (TOPIC 'bids')
KEY FORMAT TEXT
VALUE FORMAT JSON
WITH (SIZE = '1');

/* To create the source directly into the cluster previously created:
CREATE
SOURCE IF NOT EXISTS bids_source
IN CLUSTER nexmark
FROM KAFKA CONNECTION kafka_connection (TOPIC 'bids')
KEY FORMAT TEXT
VALUE FORMAT JSON
 */

CREATE
SOURCE IF NOT EXISTS people_source
FROM KAFKA CONNECTION kafka_connection (TOPIC 'people')
KEY FORMAT TEXT
VALUE FORMAT JSON
WITH (SIZE = '1');

CREATE
SOURCE IF NOT EXISTS auctions_source
FROM KAFKA CONNECTION kafka_connection (TOPIC 'auctions')
KEY FORMAT TEXT
VALUE FORMAT JSON
WITH (SIZE = '1');

/* For Materialize Cloud:
CREATE
SOURCE IF NOT EXISTS auctions_source
FROM KAFKA CONNECTION kafka_connection (TOPIC 'auctions')
KEY FORMAT TEXT
VALUE FORMAT JSON
WITH (SIZE = 'medium')
 */