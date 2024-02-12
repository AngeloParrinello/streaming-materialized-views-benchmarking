package it.agilelab.thesis.nexmark.kafka.presentation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.agilelab.thesis.nexmark.jackson.JacksonUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Objects;

public class JacksonSerializer<T> implements Serializer<T> {
    private ObjectMapper objectMapper;

    public JacksonSerializer() {
        this.objectMapper = JacksonUtils.getMapper();
    }

    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // intentionally left blank because no further configuration is needed
    }

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param data  typed data
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(final String topic, final T data) {
        this.configure(null, false);
        if (data == null) {
            return null;
        }

        try {
            return this.objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }

    /**
     * Close this serializer.
     * <p>
     * This method must be idempotent as it may be called multiple times.
     */
    @Override
    public void close() {
        Serializer.super.close();
    }

    /**
     * Get the {@link ObjectMapper} used by this serializer.
     *
     * @return the {@link ObjectMapper} used by this serializer
     */
    public ObjectMapper getObjectMapper() {
        return this.objectMapper;
    }

    /**
     * Set the {@link ObjectMapper} used by this serializer.
     *
     * @param objectMapper the {@link ObjectMapper} used by this serializer
     */
    public void setObjectMapper(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Generated equals method for comparing two objects.
     *
     * @param o the object to compare with.
     * @return true if those two objects are the same.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JacksonSerializer)) {
            return false;
        }
        JacksonSerializer<?> that = (JacksonSerializer<?>) o;
        return Objects.equals(getObjectMapper(), that.getObjectMapper());
    }

    /**
     * Generated hashCode method, used to generate the hash values of objects.
     *
     * @return an integer whose value represents the hash value of the input object
     */
    @Override
    public int hashCode() {
        return Objects.hash(getObjectMapper());
    }

    /**
     * Generated toString method, used to print object's internal value.
     *
     * @return the string related to the object.
     */
    @Override
    public String toString() {
        return "JacksonSerializer{"
                + "objectMapper=" + this.objectMapper
                + '}';
    }
}
