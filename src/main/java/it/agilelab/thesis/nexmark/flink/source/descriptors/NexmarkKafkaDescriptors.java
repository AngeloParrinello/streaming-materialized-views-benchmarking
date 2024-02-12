package it.agilelab.thesis.nexmark.flink.source.descriptors;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;

public final class NexmarkKafkaDescriptors {

    private static final Schema BID_SCHEMA = Schema.newBuilder()
            .column("auction", DataTypes.BIGINT().notNull())
            .column("bidder", DataTypes.BIGINT().notNull())
            .column("price", DataTypes.BIGINT().notNull())
            .column("channel", DataTypes.STRING())
            .column("url", DataTypes.STRING())
            .column("entryTime", DataTypes.TIMESTAMP_LTZ(3))
            .column("extra", DataTypes.STRING())
            .primaryKey("auction", "bidder")
            // set some watermark strategy here
            .watermark("entryTime", "entryTime - INTERVAL '5' SECOND")
            .build();
    private static final Schema AUCTION_SCHEMA = Schema.newBuilder()
            .column("id", DataTypes.BIGINT().notNull())
            .column("itemName", DataTypes.STRING())
            .column("description", DataTypes.STRING())
            .column("initialBid", DataTypes.BIGINT().notNull())
            .column("reserve", DataTypes.BIGINT().notNull())
            .column("entryTime", DataTypes.TIMESTAMP_LTZ(3))
            .column("expirationTime", DataTypes.TIMESTAMP_LTZ(3))
            .column("seller", DataTypes.BIGINT().notNull())
            .column("category", DataTypes.BIGINT().notNull())
            .column("extra", DataTypes.STRING())
            .primaryKey("id")
            // set some watermark strategy here
            .watermark("entryTime", "entryTime - INTERVAL '5' SECOND")
            .build();
    private static final Schema PERSON_SCHEMA = Schema.newBuilder()
            .column("id", DataTypes.BIGINT().notNull())
            .column("name", DataTypes.STRING())
            .column("emailAddress", DataTypes.STRING())
            .column("creditCard", DataTypes.STRING())
            .column("city", DataTypes.STRING())
            .column("state", DataTypes.STRING())
            .column("entryTime", DataTypes.TIMESTAMP_LTZ(3))
            .column("extra", DataTypes.STRING())
            .primaryKey("id")
            // set some watermark strategy here
            .watermark("entryTime", "entryTime - INTERVAL '5' SECOND")
            .build();
    private static final TableDescriptor BID_DESCRIPTOR = TableDescriptor.forConnector("kafka")
            .schema(BID_SCHEMA)
            .option("key.format", "json")
            .option("value.format", "json")
            .option("topic", "bids")
            .option("scan.startup.mode", "earliest-offset")
            // set the Kafka server address here
            .option("properties.bootstrap.servers", "localhost:29092")
            .build();
    private static final TableDescriptor AUCTION_DESCRIPTOR = TableDescriptor.forConnector("kafka")
            .schema(AUCTION_SCHEMA)
            .option("key.format", "json")
            .option("value.format", "json")
            .option("topic", "auctions")
            .option("scan.startup.mode", "earliest-offset")
            // set the Kafka server address here
            .option("properties.bootstrap.servers", "localhost:29092")
            .build();
    private static final TableDescriptor PERSON_DESCRIPTOR = TableDescriptor.forConnector("kafka")
            .schema(PERSON_SCHEMA)
            .option("key.format", "json")
            .option("value.format", "json")
            .option("topic", "people")
            .option("scan.startup.mode", "earliest-offset")
            // set the Kafka server address here
            .option("properties.bootstrap.servers", "localhost:29092")
            .build();

    private NexmarkKafkaDescriptors() {
    }

    /**
     * Returns the schema of the auction table.
     *
     * @return the schema of the auction table
     */
    public static Schema getAuctionSchema() {
        return AUCTION_SCHEMA;
    }

    /**
     * Returns the schema of the bid table.
     *
     * @return the schema of the bid table
     */
    public static Schema getBidSchema() {
        return BID_SCHEMA;
    }

    /**
     * Returns the schema of the person table.
     *
     * @return the schema of the person table
     */
    public static Schema getPersonSchema() {
        return PERSON_SCHEMA;
    }

    /**
     * Returns the descriptor of the auction table.
     *
     * @return the descriptor of the auction table
     */
    public static TableDescriptor getAuctionDescriptor() {
        return AUCTION_DESCRIPTOR;
    }

    /**
     * Returns the descriptor of the bid table.
     *
     * @return the descriptor of the bid table
     */
    public static TableDescriptor getBidDescriptor() {
        return BID_DESCRIPTOR;
    }

    /**
     * Returns the descriptor of the person table.
     *
     * @return the descriptor of the person table
     */
    public static TableDescriptor getPersonDescriptor() {
        return PERSON_DESCRIPTOR;
    }
}
