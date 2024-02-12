package it.agilelab.thesis.nexmark.flink;

import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.NexmarkTestsUtil;
import it.agilelab.thesis.nexmark.NexmarkUtil;
import it.agilelab.thesis.nexmark.flink.presentation.NextEventKafkaDeserializationSchema;
import it.agilelab.thesis.nexmark.flink.presentation.NextEventKafkaSerializationSchema;
import it.agilelab.thesis.nexmark.flink.source.NexmarkGeneratorSource;
import it.agilelab.thesis.nexmark.flink.source.descriptors.NexmarkKafkaDescriptors;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.model.Auction;
import it.agilelab.thesis.nexmark.model.Bid;
import it.agilelab.thesis.nexmark.model.NextEvent;
import it.agilelab.thesis.nexmark.model.Person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.List;

public class NexmarkQueriesFlinkTest {

    private static final NexmarkTestsUtil NEXMARK_TESTS_UTIL = new NexmarkTestsUtil();

    private static final NexmarkConfiguration NEXMARK_CONFIGURATION = new NexmarkConfiguration();
    private static final GeneratorConfig GENERATOR_CONFIG = new GeneratorConfig(NEXMARK_CONFIGURATION,
            System.currentTimeMillis(), 1, 5000, 1);
    private static final String BIDS_TOPIC = "bids";
    private static final String AUCTIONS_TOPIC = "auctions";
    private static final String PEOPLE_TOPIC = "people";

    private StreamTableEnvironment tableEnv;

    private DataStream<Bid> bidDataStream;
    private DataStream<Auction> auctionDataStream;
    private DataStream<Person> personDataStream;

    @BeforeAll
    public static void createEventsOnKafka() throws Exception {

        NEXMARK_TESTS_UTIL.startKafkaContainer();

        String bootstrapServers = NEXMARK_TESTS_UTIL.getKafkaBootstrapServers();

        NexmarkGeneratorSource source = new NexmarkGeneratorSource(GENERATOR_CONFIG);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<NextEvent> stream = env.addSource(source);

        KafkaSink<NextEvent> sink = KafkaSink.<NextEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new NextEventKafkaSerializationSchema())
                .build();

        stream.sinkTo(sink);
        env.execute();
    }

    @BeforeEach
    void setup() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        tableEnv = StreamTableEnvironment.create(env);

        KafkaSource<NextEvent> bidsSource = createKafkaSource(BIDS_TOPIC);
        KafkaSource<NextEvent> auctionsSource = createKafkaSource(AUCTIONS_TOPIC);
        KafkaSource<NextEvent> peopleSource = createKafkaSource(PEOPLE_TOPIC);

        bidDataStream = env.fromSource(bidsSource, WatermarkStrategy.noWatermarks(), BIDS_TOPIC)
                .map(x -> (Bid) x.getActualEvent().getActualEvent());
        auctionDataStream = env.fromSource(auctionsSource, WatermarkStrategy.noWatermarks(), AUCTIONS_TOPIC)
                .map(x -> (Auction) x.getActualEvent().getActualEvent());
        personDataStream = env.fromSource(peopleSource, WatermarkStrategy.noWatermarks(), PEOPLE_TOPIC)
                .map(x -> (Person) x.getActualEvent().getActualEvent());
    }

    @Test
    void query1Test() {
        String query1Path = "src/main/resources/queries/flink/query1.sql";
        queryCheck(query1Path);
    }

    @Test
    void query2Test() {

        String query2Path = "src/main/resources/queries/flink/query2.sql";

        queryCheck(query2Path);
    }

    @Test
    void query3Test() {
        String query3Path = "src/main/resources/queries/flink/query3.sql";
        queryCheck(query3Path);
    }

    @Test
    void query4Test() {
        String query4Path = "src/main/resources/queries/flink/query4.sql";
        queryCheck(query4Path);
    }

    @Test
    void query5Test() {
        String query5Path = "src/main/resources/queries/flink/query5.sql";
        queryCheck(query5Path);
    }

    // Remember that this query here is not the real one
    @Test
    @Disabled
    void query6Test() {
        String query6Path = "src/main/resources/queries/flink/query6.sql";
        queryCheck(query6Path);
    }

    @Test
    void query7Test() {
        String query7Path = "src/main/resources/queries/flink/query7.sql";
        queryCheck(query7Path);
    }

    @Test
    void query8Test() {
        String query8Path = "src/main/resources/queries/flink/query8.sql";
        queryCheck(query8Path);
    }

    private void queryCheck(String queryPath) {
        try {
            List<String> query = NexmarkUtil.readQueryFileAsString(queryPath);
            String createTableQuery = query.get(0);
            String actualQuery = query.get(1);

            createTable(tableEnv, createTableQuery);

            executeAndVerifyQuery(actualQuery);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void executeAndVerifyQuery(String query) {
        tableEnv.createTemporaryView(BIDS_TOPIC, bidDataStream, NexmarkKafkaDescriptors.getBidSchema());
        tableEnv.createTemporaryView(AUCTIONS_TOPIC, auctionDataStream, NexmarkKafkaDescriptors.getAuctionSchema());
        tableEnv.createTemporaryView(PEOPLE_TOPIC, personDataStream, NexmarkKafkaDescriptors.getPersonSchema());

        TableResult result = tableEnv.executeSql(query);
        int rowCount = collectAndPrintResults(result);

        Assertions.assertEquals(1, rowCount);
    }

    private int collectAndPrintResults(TableResult result) {
        CloseableIterator<Row> iterator = result.collect();
        int count = 0;
        while (iterator.hasNext()) {
            count++;
            System.out.println("Row: " + iterator.next());
        }
        return count;
    }

    private KafkaSource<NextEvent> createKafkaSource(final String inputTopic) {
        return KafkaSource.<NextEvent>builder()
                .setBootstrapServers(NEXMARK_TESTS_UTIL.getKafkaBootstrapServers())
                .setTopics(inputTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new NextEventKafkaDeserializationSchema())
                .setBounded(OffsetsInitializer.latest())
                .build();
    }


    private void createTable(StreamTableEnvironment tableEnv, String queryTable) {
        tableEnv.executeSql(queryTable);
    }
}
