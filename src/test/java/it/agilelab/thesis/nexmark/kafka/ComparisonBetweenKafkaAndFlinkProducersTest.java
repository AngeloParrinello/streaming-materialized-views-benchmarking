package it.agilelab.thesis.nexmark.kafka;

import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.flink.presentation.NextEventKafkaSerializationSchema;
import it.agilelab.thesis.nexmark.flink.source.NexmarkGeneratorSource;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.model.NextEvent;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Properties;

@Disabled
public class ComparisonBetweenKafkaAndFlinkProducersTest {
    private final static long numEvents = 10_000_000L;

    private final static long transactionSize = 100_000L;

    private static NexmarkKafkaProducer kafkaProducer;

    private static NexmarkGeneratorSource flinkProducer;

    @BeforeAll
    static void setup() {
        GeneratorConfig config = new GeneratorConfig(new NexmarkConfiguration(),
                System.currentTimeMillis(), 1, numEvents, 1);
        kafkaProducer = new NexmarkKafkaProducer("localhost:29092", config, transactionSize);

        flinkProducer = new NexmarkGeneratorSource(config);
    }

    @Test
    void testKafkaProducer() {
        var startTime = System.currentTimeMillis();
        kafkaProducer.start();
        var endTime = System.currentTimeMillis();
        System.out.println("Kafka producer started in " + (endTime - startTime) + " ms");
    }

    @Test
    void testFlinkProducer() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        env.enableCheckpointing(60_000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStream<NextEvent> stream = env.addSource(flinkProducer);

        var producerProperties = new Properties();
        producerProperties.setProperty("transaction.timeout.ms", "60000");

        KafkaSink<NextEvent> sink = KafkaSink.<NextEvent>builder()
                .setBootstrapServers("localhost:29092")
                .setKafkaProducerConfig(producerProperties)
                .setRecordSerializer(new NextEventKafkaSerializationSchema())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        stream.sinkTo(sink);
        env.execute();

    }

}
