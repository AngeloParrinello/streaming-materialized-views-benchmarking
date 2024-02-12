package it.agilelab.thesis.nexmark.flink.presentation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.agilelab.thesis.nexmark.NexmarkUtil;
import it.agilelab.thesis.nexmark.jackson.JacksonUtils;
import it.agilelab.thesis.nexmark.model.NextEvent;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;


public class NextEventKafkaSerializationSchema implements KafkaRecordSerializationSchema<NextEvent> {
    private transient ObjectMapper mapper;

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #serialize(NextEvent, KafkaSinkContext, Long)} and thus suitable for one time setup work.
     *
     * <p>The provided {@link SerializationSchema.InitializationContext} can be used to access
     * additional features such as e.g. registering user metrics.
     *
     * @param context     Contextual information that can be used during initialization.
     * @param sinkContext runtime information i.e. partitions, subtaskId
     */
    @Override
    public void open(final SerializationSchema.InitializationContext context,
                     final KafkaSinkContext sinkContext) throws Exception {
        KafkaRecordSerializationSchema.super.open(context, sinkContext);
        this.mapper = JacksonUtils.getMapper();
    }

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
    public ProducerRecord<byte[], byte[]> serialize(final NextEvent element,
                                                    final KafkaSinkContext context,
                                                    final Long timestamp) {
        if (this.mapper == null) {
            this.mapper = JacksonUtils.getMapper();
        }
        byte[] value;
        try {
            value = this.mapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        String topic = NexmarkUtil.selectTopic(element);
        byte[] key = NexmarkUtil.selectKey(element).getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(topic, key, value);
    }
}
