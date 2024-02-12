package it.agilelab.thesis.nexmark.flink.source;

import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.NexmarkTestsUtil;
import it.agilelab.thesis.nexmark.flink.presentation.NextEventKafkaDeserializationSchema;
import it.agilelab.thesis.nexmark.flink.presentation.NextEventKafkaSerializationSchema;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.model.NextEvent;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Testcontainers(disabledWithoutDocker = true, parallel = true)
public class NexmarkGeneratorSourceTest {
    private static final NexmarkConfiguration nexmarkConfiguration = new NexmarkConfiguration();
    private static final GeneratorConfig generatorConfig = new GeneratorConfig(nexmarkConfiguration,
            System.currentTimeMillis(), 1, 100, 1);

    private static final NexmarkTestsUtil nexmarkTestsUtil = new NexmarkTestsUtil();
    private static NexmarkGeneratorSource source;


    @BeforeEach
    void setup() {
        source = new NexmarkGeneratorSource(generatorConfig);
    }

    @Test
    @Order(1)
    void generateAndPrintTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.addSource(source).addSink(new PrintSinkFunction<>());
        JobExecutionResult executionResult = env.execute();
        assertEquals(100, (Long) executionResult.getAccumulatorResult("eventsCreatedSoFar"));
    }

    @Test
    @Order(2)
    void generateAndSinkOnKafka() throws Exception {
        nexmarkTestsUtil.startKafkaContainer();

        String kafkaBootstrapServers = nexmarkTestsUtil.getKafkaBootstrapServers();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(10_000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // Must restart a job from a checkpoint to avoid duplicate events
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // File based Backend
        //env.setStateBackend(new FsStateBackend(Paths.get(<local file system path goes here>).toUri, false))

        DataStream<NextEvent> stream = env.addSource(source);

        var producerProperties = new Properties();
        producerProperties.setProperty("transaction.timeout.ms", "60000");

        KafkaSink<NextEvent> sink = KafkaSink.<NextEvent>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setKafkaProducerConfig(producerProperties)
                .setRecordSerializer(new NextEventKafkaSerializationSchema())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        stream.sinkTo(sink);
        JobExecutionResult executionResult = env.execute();

        assertEquals(100, (Long) executionResult.getAccumulatorResult("eventsCreatedSoFar"));
    }

    @Test()
    @Order(3)
    void consumeFromKafka() throws Exception {
        // consume events from kafka
        String bootstrapServers = nexmarkTestsUtil.getKafkaBootstrapServers();
        String inputTopic = "bids";
        String groupId = "nexmark";

        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        KafkaSourceBuilder<NextEvent> sourceBuilder = KafkaSource.<NextEvent>builder()
                .setTopics(inputTopic)
                .setDeserializer(new NextEventKafkaDeserializationSchema())
                .setProperties(properties)
                .setBounded(OffsetsInitializer.latest())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST));

        KafkaSource<NextEvent> kafkaSource = sourceBuilder.build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        DataStream<NextEvent> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source");

        var producerProperties = new Properties();
        producerProperties.setProperty("transaction.timeout.ms", "60000");

        // TODO: Why do I need to sink to kafka again?
        stream.sinkTo(KafkaSink.<NextEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setKafkaProducerConfig(producerProperties)
                .setRecordSerializer(new NextEventKafkaSerializationSchema())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build());

        List<NextEvent> list = stream.executeAndCollect(100);
        list.forEach(bid -> System.out.println("The Bid is: " + bid));
        // we've set the number of events to 100, so we should have 100 events
        // 2 people events
        // 6 auction events
        // 92 bid events
        // at least, the count will be 92 since at least one time the generator will generate some bid events
        assertTrue(list.size() >= 92);
    }

}