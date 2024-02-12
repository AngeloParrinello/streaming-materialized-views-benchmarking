package it.agilelab.thesis.nexmark.flink.source;

import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.NexmarkTestsUtil;
import it.agilelab.thesis.nexmark.flink.presentation.NextEventKafkaDeserializationSchema;
import it.agilelab.thesis.nexmark.flink.presentation.NextEventKafkaSerializationSchema;
import it.agilelab.thesis.nexmark.flink.source.descriptors.NexmarkKafkaDescriptors;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.model.Auction;
import it.agilelab.thesis.nexmark.model.Bid;
import it.agilelab.thesis.nexmark.model.NextEvent;
import it.agilelab.thesis.nexmark.model.Person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

public class NexmarkKafkaDescriptorsTest {

    private static final NexmarkTestsUtil NEXMARK_TESTS_UTIL = new NexmarkTestsUtil();

    private static final Bid BID = new Bid(1,
            1, 10, "abc123", "url123", Instant.now(), "Some extra data");
    private static final Bid BID2 = new Bid(2,
            2, 20, "abc123", "url123", Instant.now(), "Some extra data");
    private static final Bid BID3 = new Bid(3,
            3, 30, "abc123", "url123", Instant.now(), "Some extra data");

    private static final Person PERSON = new Person(1, "John Doe",
            "john.doe@example.com", "1234567890", "New York",
            "NY", Instant.now(), "Some extra data");

    private static final Person PERSON2 = new Person(2, "Micky Mouse",
            "micky.mouse@example.com", "0987654321", "Los Angeles",
            "CA", Instant.now(), "Some extra data");

    private static final Person PERSON3 = new Person(3, "Donald Duck",
            "donald.duck@example.com", "1357924680", "Chicago",
            "IL", Instant.now(), "Some extra data");

    private static final Auction AUCTION = new Auction(1,
            "abc123", "desc123", 100, 50, Instant.now(),
            Instant.now(), 1, 1, "Some extra data");

    private static final Auction AUCTION2 = new Auction(2,
            "abc123", "desc123", 200, 100, Instant.now(),
            Instant.now(), 2, 2, "Some extra data");

    private static final Auction AUCTION3 = new Auction(3,
            "abc123", "desc123", 300, 150, Instant.now(),
            Instant.now(), 3, 3, "Some extra data");

    private static final Bid[] BIDS = new Bid[]{BID, BID2, BID3};

    private static final Person[] PERSONS = new Person[]{PERSON, PERSON2, PERSON3};

    private static final Auction[] AUCTIONS = new Auction[]{AUCTION, AUCTION2, AUCTION3};

    private static final NexmarkConfiguration NEXMARK_CONFIGURATION = new NexmarkConfiguration();
    private static final GeneratorConfig GENERATOR_CONFIG = new GeneratorConfig(NEXMARK_CONFIGURATION,
            System.currentTimeMillis(), 1, 100, 1);
    private static NexmarkGeneratorSource source;

    @BeforeAll
    public static void createEventsOnKafka() throws Exception {

        NEXMARK_TESTS_UTIL.startKafkaContainer();

        String bootstrapServers = NEXMARK_TESTS_UTIL.getKafkaBootstrapServers();

        source = new NexmarkGeneratorSource(GENERATOR_CONFIG);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<NextEvent> stream = env.addSource(source);

        KafkaSink<NextEvent> sink = KafkaSink.<NextEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new NextEventKafkaSerializationSchema())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        stream.sinkTo(sink);
        env.execute();
    }

    @BeforeEach
    final void setup() {
        source = new NexmarkGeneratorSource(GENERATOR_CONFIG);
    }


    @Test
    void testSelectAllMinimal() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a DataStream
        DataStream<Bid> dataStream = env.fromElements(BIDS);

        // interpret the insert-only DataStream as a Table
        Table inputTable = tableEnv.fromDataStream(dataStream);
        inputTable.printSchema();

        // register the Table object as a view and query it
        tableEnv.createTemporaryView("InputTable", inputTable);
        tableEnv.executeSql("SELECT * FROM InputTable").print();
    }

    @Test
    void testHandlingOfInsertOnlyDataStreamBids() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a DataStream
        DataStream<Bid> dataStream = env.fromElements(BIDS);

        // interpret the insert-only DataStream as a Table
        Table inputTable = tableEnv.fromDataStream(dataStream, NexmarkKafkaDescriptors.getBidSchema());
        inputTable.printSchema();

        // register the Table object as a view and query it
        tableEnv.createTemporaryView("InputTable", inputTable);
        tableEnv.executeSql("SELECT * FROM InputTable").print();

        Table bidTable = tableEnv.from(NexmarkKafkaDescriptors.getBidDescriptor());
        bidTable.printSchema();

        Table auctionTable = tableEnv.from(NexmarkKafkaDescriptors.getAuctionDescriptor());
        auctionTable.printSchema();

        Table personTable = tableEnv.from(NexmarkKafkaDescriptors.getPersonDescriptor());
        personTable.printSchema();
    }

    @Test
    void testHandlingOfInsertOnlyDataStreamPersons() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a DataStream
        DataStream<Person> dataStream = env.fromElements(PERSONS);

        // interpret the insert-only DataStream as a Table
        Table inputTable = tableEnv.fromDataStream(dataStream, NexmarkKafkaDescriptors.getPersonSchema());
        inputTable.printSchema();

        // register the Table object as a view and query it
        tableEnv.createTemporaryView("InputTable", inputTable);
        tableEnv.executeSql("SELECT * FROM InputTable").print();
    }

    @Test
    void testHandlingOfInsertOnlyDataStreamAuctions() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a DataStream
        DataStream<Auction> dataStream = env.fromElements(AUCTIONS);

        // interpret the insert-only DataStream as a Table
        Table inputTable = tableEnv.fromDataStream(dataStream, NexmarkKafkaDescriptors.getAuctionSchema());
        inputTable.printSchema();

        // register the Table object as a view and query it
        tableEnv.createTemporaryView("InputTable", inputTable);
        tableEnv.executeSql("SELECT * FROM InputTable").print();
    }

    @Test
    void testTemporaryView() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a DataStream
        DataStream<Auction> dataStream = env.fromElements(AUCTIONS);

        tableEnv.createTemporaryView("Auctions", dataStream, NexmarkKafkaDescriptors.getAuctionSchema());
        tableEnv.from("Auctions").printSchema();
    }

    @Test
    void testToDataStreamsWithDifferentScenarios() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a DataStream
        // note that in the line below we are using the kafka connector
        // tableEnv.createTable("Auctions", NexmarkKafkaDescriptors.getAuctionDescriptor());

        // we can do the same thing using the SQL DDL
        // whereas here we are using the datagen connector
        // The DataGen connector allows for creating tables based on in-memory data generation.
        // This is useful when developing queries locally without access to external systems such as Kafka.
        tableEnv.executeSql("CREATE TABLE Auctions ("
                + "auctionId BIGINT, "
                + "itemName STRING, "
                + "description STRING, "
                + "initialBid BIGINT, "
                + "reserve BIGINT, "
                + "entryTime TIMESTAMP_LTZ(3), "
                + "expirationTime TIMESTAMP_LTZ(3), "
                + "seller BIGINT, "
                + "category BIGINT, "
                + "extra STRING, "
                + "watermark for entryTime as entryTime - INTERVAL '5' SECOND"
                + ") WITH ('connector' = 'datagen')");

        Table auctions = tableEnv.from("Auctions");
        auctions.printSchema();

        DataStream<Row> rowStream = tableEnv.toDataStream(auctions);
        rowStream.executeAndCollect(10).forEach(System.out::println);

        DataStream<Auction> dataStream = tableEnv.toDataStream(auctions, Auction.class);
        dataStream.executeAndCollect(10).forEach(System.out::println);

    }


    @Test
    void testTableAPINexmarkGenerator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // switch to batch mode on demand
        // this flag execute under the hood a lot of components to make the data streaming api pipeline
        // very efficient
        // env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String topic = "auctions";

        KafkaSource<NextEvent> kafkaSource = KafkaSource.<NextEvent>builder()
                .setBootstrapServers(NEXMARK_TESTS_UTIL.getKafkaBootstrapServers())
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new NextEventKafkaDeserializationSchema())
                .setBounded(OffsetsInitializer.latest())
                .build();

        List<NextEvent> events = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), topic).executeAndCollect(700);

        events.forEach(x -> System.out.println("Auction event: " + x.getActualEvent().getActualEvent()));

        DataStream<Auction> auctionDataStream =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), topic)
                        .map(x -> (Auction) x.getActualEvent().getActualEvent());

        tableEnv.createTemporaryView(topic, auctionDataStream, NexmarkKafkaDescriptors.getAuctionSchema());

        CloseableIterator<Row> iterator = tableEnv.executeSql("SELECT * FROM auctions").collect();
        int count = 0;
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            count++;
        }
        Assertions.assertEquals(count, 6);
    }
}
