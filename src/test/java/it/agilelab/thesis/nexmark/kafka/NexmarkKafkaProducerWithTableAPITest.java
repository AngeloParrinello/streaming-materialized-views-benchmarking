package it.agilelab.thesis.nexmark.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.NexmarkTestsUtil;
import it.agilelab.thesis.nexmark.flink.presentation.NextEventKafkaDeserializationSchema;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.jackson.JacksonUtils;
import it.agilelab.thesis.nexmark.model.NextEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Properties;

class NexmarkKafkaProducerWithTableAPITest {
    private static NexmarkKafkaProducer nexmarkProducer;
    private static GeneratorConfig config;

    private static NexmarkTestsUtil utils;

    @BeforeAll
    static void setup() {
        utils = new NexmarkTestsUtil();
        utils.startKafkaContainer();

        config = new GeneratorConfig(new NexmarkConfiguration(),
                System.currentTimeMillis(), 1, 200, 1);
        nexmarkProducer = new NexmarkKafkaProducer(utils.getKafkaBootstrapServers(), config, 100);
        nexmarkProducer.start();
    }

    @Test
    void producerWithTableAPI() throws Exception {
        var inputTopic = "auctions";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance();
        builder.inBatchMode();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, builder.build());

        // Configure Kafka consumer properties
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", utils.getKafkaBootstrapServers());
        kafkaProps.setProperty("group.id", "nexmark");

        // Create a Kafka consumer
        KafkaSource<NextEvent> kafkaSource = KafkaSource.<NextEvent>builder()
                .setTopics(inputTopic)
                .setProperties(kafkaProps)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new NextEventKafkaDeserializationSchema())
                .setBounded(OffsetsInitializer.latest())
                .build();

        DataStream<NextEvent> stream =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), inputTopic);

        // Consume data from Kafka and register it as a table
        tableEnv.createTemporaryView(inputTopic, stream);

        // Define SQL query
        String sqlQuery = "SELECT * FROM auctions";

        // Execute the SQL query
        Table resultTable = tableEnv.sqlQuery(sqlQuery);

        // Convert the result table to a stream of Rows
        DataStream<Row> convertedStream = tableEnv.toDataStream(resultTable);

        var producerProperties = new Properties();
        producerProperties.setProperty("transaction.timeout.ms", "60000");

        var serializer = new KafkaRecordSerializationSchema<Row>() {

            /**
             * Serializes given element and returns it as a {@link ProducerRecord}.
             *
             * @param element   element to be serialized
             * @param context   context to possibly determine target partition
             * @param timestamp timestamp
             * @return Kafka {@link ProducerRecord} or null if the given element cannot be serialized
             */
            @Nullable
            @Override
            public ProducerRecord<byte[], byte[]> serialize(Row element, KafkaSinkContext context, Long timestamp) {
                try {
                    return new ProducerRecord<>(inputTopic, JacksonUtils.getMapper().writeValueAsBytes(element));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        convertedStream.printToErr();

        convertedStream.sinkTo(KafkaSink.<Row>builder()
                .setBootstrapServers(utils.getKafkaBootstrapServers())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(producerProperties)
                .setRecordSerializer(serializer)
                .build());

        // Start the execution
        env.execute("Flink Table Example");

    }

    /*@Test
    void testTableAPINexmarkGenerator() throws Exception {
        String topic = "auctions";

        KafkaSource<NextEvent> kafkaSource = KafkaSource.<NextEvent>builder()
                .setBootstrapServers(utils.getKafkaBootstrapServers())
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new NextEventKafkaDeserializationSchema())
                .setGroupId("nexmark")
                .setBounded(OffsetsInitializer.latest())
                .build();

        // Apache Flink is able to guarantee that events will be processed exactly once when
        // used with supported sources and sinks.
        // This means that even in case of a failure where
        // Flink retries to send the same event, the consumer of that event will still receive the event only once.
        // Configuration config = new Configuration();
        // config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Checkpointing is enabled to avoid that data will be lost in case of a Task Manager failure.
        // the checkpointing interval is set to every 10 seconds.
        // The Flink Kafka producer will commit offsets as part of the checkpoint. This is not needed for Flink to
        // guarantee exactly-once results, but can be useful for other applications that use offsets for monitoring purposes.
        //env.enableCheckpointing(10_000);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Properties properties = new Properties();
        // properties.setProperty("transaction.timeout.ms", "60_000");

        DataStream<NextEvent> dataStream =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), topic);

        DataStream<Auction> auctionDataStream = dataStream.map(x -> (Auction) x.getActualEvent().getActualEvent());

        /*KafkaRecordSerializationSchema<Auction> serializer = (element, context, timestamp) -> {
            var mapper = JacksonUtils.getMapper();
            byte[] jsonByte;
            try {
                jsonByte = mapper.writeValueAsBytes(element);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            return new ProducerRecord<>("bids", jsonByte);
        };

        /*KafkaSink<Auction> sink =
                KafkaSink.<Auction>builder()
                        .setBootstrapServers(utils.getKafkaBootstrapServers())
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix("KafkaExactlyOnce")
                        .setKafkaProducerConfig(properties)
                        .setRecordSerializer(serializer)
                        .build();

        KafkaSink<NextEvent> sinkNextEvent =
                KafkaSink.<NextEvent>builder()
                        .setBootstrapServers(utils.getKafkaBootstrapServers())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setTransactionalIdPrefix("KafkaExactlyOnceSinkNextEvent")
                        .setRecordSerializer(new NextEventKafkaSerializationSchema())
                        .build();

        // auctionDataStream.sinkTo(sink);

        dataStream.sinkTo(sinkNextEvent);

        // dataStream.sinkTo(new PrintSink<>(true));

        tableEnv.createTemporaryView(topic, auctionDataStream, NexmarkKafkaDescriptors.getAuctionSchema());

        tableEnv.executeSql("SELECT * FROM auctions").print();

        //env.execute();

    }*/
}
