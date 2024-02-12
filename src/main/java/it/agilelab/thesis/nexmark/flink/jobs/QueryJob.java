package it.agilelab.thesis.nexmark.flink.jobs;

import it.agilelab.thesis.nexmark.NexmarkUtil;
import it.agilelab.thesis.nexmark.YamlParser;
import it.agilelab.thesis.nexmark.flink.presentation.NextEventKafkaDeserializationSchema;
import it.agilelab.thesis.nexmark.flink.source.descriptors.NexmarkKafkaDescriptors;
import it.agilelab.thesis.nexmark.model.Auction;
import it.agilelab.thesis.nexmark.model.Bid;
import it.agilelab.thesis.nexmark.model.NextEvent;
import it.agilelab.thesis.nexmark.model.Person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public final class QueryJob {

    private QueryJob() {
        throw new IllegalStateException("Utility class");
    }

    public static void main(final String[] args) throws IOException {

        ParameterTool parameters = ParameterTool.fromArgs(args);

        int queryNumber = parameters.getInt("queryNumber", 1);
        int parallelism = parameters.getInt("parallelism", 8);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // for DML Operations in a sync way, default is async
        tableEnv.getConfig().set("table.dml-sync", "true");

        env.disableOperatorChaining();

        String queryPath = "src/main/resources/queries/flink/query" + queryNumber + ".sql";
        String yamlConfPath = "config/nexmark-conf.yaml";
        YamlParser yamlParser = new YamlParser();
        yamlParser.parse(yamlConfPath);
        String bootstrapServers = yamlParser.getValue("nexmark.kafka.bootstrap.servers");
        String kafkaPropertiesPath = yamlParser.getValue("nexmark.kafka.producer.properties.path");

        Properties properties = new Properties();
        properties.setProperty("transaction.timeout.ms", "60000");
        properties.setProperty("group.id", "nexmark-flink-query" + queryNumber);
        properties.setProperty("register.consumer.metrics", "true");
        if (kafkaPropertiesPath != null) {
            properties.load(new FileInputStream(kafkaPropertiesPath));
        }

        env.setParallelism(parallelism);
        env.enableCheckpointing(180_000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(180_000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // Must restart a job from a checkpoint to avoid duplicate events
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        KafkaSource<NextEvent> bidsSource = createKafkaSource(properties, bootstrapServers, TopicMetaData.BIDS.getDisplayName());
        KafkaSource<NextEvent> auctionsSource = createKafkaSource(properties, bootstrapServers, TopicMetaData.AUCTIONS.getDisplayName());
        KafkaSource<NextEvent> peopleSource = createKafkaSource(properties, bootstrapServers, TopicMetaData.PEOPLE.getDisplayName());

        DataStream<Bid> bidDataStream = env.fromSource(bidsSource, WatermarkStrategy.noWatermarks(), TopicMetaData.BIDS.getDisplayName())
                .map(x -> (Bid) x.getActualEvent().getActualEvent());
        DataStream<Auction> auctionDataStream = env.fromSource(auctionsSource, WatermarkStrategy.noWatermarks(), TopicMetaData.AUCTIONS.getDisplayName())
                .map(x -> (Auction) x.getActualEvent().getActualEvent());
        DataStream<Person> personDataStream = env.fromSource(peopleSource, WatermarkStrategy.noWatermarks(), TopicMetaData.PEOPLE.getDisplayName())
                .map(x -> (Person) x.getActualEvent().getActualEvent());

        try {
            List<String> query = NexmarkUtil.readQueryFileAsString(queryPath);
            String createTableQuery = query.get(0);
            String actualQuery = query.get(1);

            tableEnv.executeSql(createTableQuery);

            tableEnv.createTemporaryView(TopicMetaData.BIDS.getDisplayName(), bidDataStream, NexmarkKafkaDescriptors.getBidSchema());
            tableEnv.createTemporaryView(TopicMetaData.AUCTIONS.getDisplayName(), auctionDataStream, NexmarkKafkaDescriptors.getAuctionSchema());
            tableEnv.createTemporaryView(TopicMetaData.PEOPLE.getDisplayName(), personDataStream, NexmarkKafkaDescriptors.getPersonSchema());

            tableEnv.executeSql(actualQuery);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static KafkaSource<NextEvent> createKafkaSource(final Properties properties, final String bootstrapServers, final String inputTopic) {
        return KafkaSource.<NextEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setProperties(properties)
                .setClientIdPrefix(UUID.randomUUID().toString())
                .setTopics(inputTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new NextEventKafkaDeserializationSchema())
                .setBounded(OffsetsInitializer.latest())
                .build();
    }

    // TODO: check why the cast does not work here
    /* private static <T> DataStream<T> getDataStreamForTopic(final StreamExecutionEnvironment env,
                                                           final TopicMetaData topicMetaData,
                                                           final KafkaSource<NextEvent> source,
                                                           final Class<T> targetType) {
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), topicMetaData.getDisplayName())
                .map(x -> targetType.cast(x.getActualEvent().getActualEvent()));
    }

    public static <T> void createTemporaryView(
            final TopicMetaData topicMetaData,
            final DataStream<T> dataStream,
            final StreamTableEnvironment tableEnv
    ) {
        tableEnv.createTemporaryView(topicMetaData.getDisplayName(), dataStream, topicMetaData.getSchema());
    }*/


    public enum TopicMetaData {
        /**
         * The name of the topic containing the {@link Auction} events.
         */
        AUCTIONS("auctions", NexmarkKafkaDescriptors.getAuctionSchema()),
        /**
         * The name of the topic containing the {@link Person} events.
         */
        PEOPLE("people", NexmarkKafkaDescriptors.getPersonSchema()),
        /**
         * The name of the topic containing the {@link Bid} events.
         */
        BIDS("bids", NexmarkKafkaDescriptors.getBidSchema());

        private final String displayName;

        private final Schema schema;

        TopicMetaData(final String displayName, final Schema schema) {
            this.displayName = displayName;
            this.schema = schema;
        }

        public String getDisplayName() {
            return this.displayName;
        }

        public Schema getSchema() {
            return this.schema;
        }
    }

}
