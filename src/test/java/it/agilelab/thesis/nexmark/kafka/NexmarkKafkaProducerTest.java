package it.agilelab.thesis.nexmark.kafka;

import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.NexmarkTestsUtil;
import it.agilelab.thesis.nexmark.flink.presentation.NextEventKafkaDeserializationSchema;
import it.agilelab.thesis.nexmark.flink.presentation.NextEventKafkaSerializationSchema;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.model.Auction;
import it.agilelab.thesis.nexmark.model.NextEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true, parallel = true)
class NexmarkKafkaProducerTest {
    private static NexmarkKafkaProducer nexmarkKafkaProducer;
    private static GeneratorConfig config;
    private static NexmarkTestsUtil utils;

    @BeforeAll
    static void setup() {
        utils = new NexmarkTestsUtil();
        utils.startKafkaContainer();

        config = new GeneratorConfig(new NexmarkConfiguration(),
                System.currentTimeMillis(), 1, 200, 1);
        nexmarkKafkaProducer = new NexmarkKafkaProducer(utils.getKafkaBootstrapServers(), config, 100);
        nexmarkKafkaProducer.start();
    }

    @Test
    void testCreateCorrectProducer() {
        assertTrue(nexmarkKafkaProducer.getProducer() instanceof KafkaProducer);
    }

    @Test
    void testCreateCorrectGenerator() {
        Assertions.assertNotNull(nexmarkKafkaProducer.getGenerator());
    }

    @Test
    void testCreateCorrectConfig() {
        Assertions.assertSame(nexmarkKafkaProducer.getConfig(), config);
    }

    @Test
    void testCreateCorrectTransactionSize() {
        Assertions.assertEquals(nexmarkKafkaProducer.getTransactionSize(), 100);
    }

    @Test
    void testCreateCorrectKafkaBootstrapServers() {
        Assertions.assertEquals(nexmarkKafkaProducer.getBootstrapServer(), utils.getKafkaBootstrapServers());
    }

    @Test
    void testCreateCorrectMetadataList() {
        Assertions.assertNotNull(nexmarkKafkaProducer.getMetadataList());
    }

    @Test
    void testProduceWithNull() {
        Assertions.assertThrows(NullPointerException.class, () -> nexmarkKafkaProducer.produce(null));
    }

    @Test
    void testStart() {
        Assertions.assertEquals(200, nexmarkKafkaProducer.getMetadataList().size());
    }

    /**
     * We read from Kafka topic and print the events.
     * <p>
     * If we remove the sink and we print the events, we can see that the events are printed
     * but the test does not finish. Why is that? Because since we do not have a sink which can commit
     * the offsets, the Kafka consumer will not commit the offsets and the test will not finish. This is due
     * to the fact that we're in a transactional context and the Kafka consumer will not commit the offsets and we will
     * always wait for the transaction to finish. Mainly reason: exactly once semantics.
     * <p>
     * Remember that the sink cannot create a new topic during the serialization phase, so you need to provide
     * a topic which already exists.
     * <p>
     * In this test the method execute() is not required because the method executeAncCollect (to use only
     * for testing purposes) already calls execute().
     * <p>
     * Checkpointing is enabled to avoid that data will be lost in case of a Task Manager failure.
     * In this test, the checkpointing interval is set to every 10 seconds. The Flink Kafka producer will commit
     * offsets as part of the checkpoint. This is not needed for Flink to guarantee exactly-once results,
     * but can be useful for other applications that use offsets for monitoring purposes.
     * <p>
     * We set the transactionalIdPrefix because this value has to
     * be unique for each Flink application that you run in the same Kafka cluster.
     * <p>
     * Remember that on Flink the transformations are lazy, so we need to call execute() to start the execution.
     */
    @Test
    void testProducerAndPrintTheEvents() throws Exception {
        // given
        String inputTopic = "auctions";

        //producerProperties.setProperty("isolation.level", "read_committed");

        KafkaSource<NextEvent> kafkaSource = KafkaSource.<NextEvent>builder()
                .setBootstrapServers(utils.getKafkaBootstrapServers())
                .setTopics(inputTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new NextEventKafkaDeserializationSchema())
                .setGroupId("nexmark")
                .setBounded(OffsetsInitializer.latest())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        DataStream<NextEvent> stream =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), inputTopic);

        var producerProperties = new Properties();
        producerProperties.setProperty("transaction.timeout.ms", "60000");


        stream.sinkTo(KafkaSink.<NextEvent>builder()
                .setBootstrapServers(utils.getKafkaBootstrapServers())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("KafkaExactlyOnce")
                .setKafkaProducerConfig(producerProperties)
                .setRecordSerializer(new NextEventKafkaSerializationSchema())
                .build());

        var iterator = stream
                .map((MapFunction<NextEvent, Auction>) value -> (Auction) value.getActualEvent().getActualEvent())
                // when
                .executeAndCollect();

        int count = 0;

        while (iterator.hasNext()) {
            var event = iterator.next();
            System.err.println(event);
            count++;
        }

        iterator.close();

        assertEquals(12, count);
    }
}
