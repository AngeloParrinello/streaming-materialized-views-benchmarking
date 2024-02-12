package it.agilelab.thesis.nexmark.flink.presentation;

import it.agilelab.thesis.nexmark.model.Bid;
import it.agilelab.thesis.nexmark.model.Event;
import it.agilelab.thesis.nexmark.model.NextEvent;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class NextEventKafkaPresentationTest {
    private NextEvent nextEvent;


    @BeforeEach
    void setup() {
        Bid bid = new Bid(1000, 1001, 73265511, "channel-5205",
                "https://www.nexmark.com/fne/owy/udhl/item.htm?query=1&channel_id=1440219136",
                Instant.parse("2023-06-13T08:23:07.636Z"), "xnecriauvallU_VUXZmvwwcujbhecwX_Z^LH");
        Event<Bid> event = new Event<>(bid);
        nextEvent = new NextEvent(1686644587636L, 1686644587636L, event,
                1686644587636L);
    }

    @Test
    void testSerialize() {
        KafkaRecordSerializationSchema<NextEvent> kafkaSerializationSchema = new NextEventKafkaSerializationSchema();
        ProducerRecord<byte[], byte[]> record = kafkaSerializationSchema.serialize(nextEvent, null, System.currentTimeMillis());
        System.out.println(record);
        assertNotNull(record);
        String nextEventJsonValue = new String(record.value(), StandardCharsets.UTF_8);
        String nextEventJsonKey = new String(record.key(), StandardCharsets.UTF_8);
        System.out.println(nextEventJsonValue);
        System.out.println(nextEventJsonKey);
    }

    @Test
    void testDeserialize() throws IOException {
        // serialize
        KafkaRecordSerializationSchema<NextEvent> kafkaSerializationSchema = new NextEventKafkaSerializationSchema();
        ProducerRecord<byte[], byte[]> producerRecord = kafkaSerializationSchema.serialize(nextEvent, null, System.currentTimeMillis());
        // Here, the partition is null because the partition is not set yet.
        // Kafka will assign the partition when the record is received. There are three ways:
        // 1. Partition is explicitly specified
        // 2. Partition is not specified but a key is present, and so a partition will be chosen based on
        // a hash of the key
        // 3. Neither key nor partition is present, and so a partition will be assigned casually
        System.out.println(producerRecord);
        assertNotNull(producerRecord);
        // deserialize
        KafkaRecordDeserializationSchema<NextEvent> kafkaRecordDeserializationSchema = new NextEventKafkaDeserializationSchema();
        ConsumerRecord<byte[], byte[]> consumerRecord =
                // The partition is manually assigned to 1 because the partition is not set yet.
                new ConsumerRecord<>(producerRecord.topic(), 1, 0,
                        producerRecord.key(), producerRecord.value());
        assertEquals(1001, Integer.valueOf(new String(consumerRecord.key(), StandardCharsets.UTF_8)));
        kafkaRecordDeserializationSchema.deserialize(consumerRecord, new Collector<>() {
            @Override
            public void collect(final NextEvent record) {
                System.out.println(record);
                assertEquals(nextEvent, record);
            }

            @Override
            public void close() {
                System.out.flush();
            }
        });

    }

}
