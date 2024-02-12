package it.agilelab.thesis.nexmark.kafka;

import it.agilelab.thesis.nexmark.NexmarkUtil;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.generator.NexmarkGenerator;
import it.agilelab.thesis.nexmark.kafka.presentation.JacksonSerializer;
import it.agilelab.thesis.nexmark.model.NextEvent;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import java.util.*;
import java.util.concurrent.Future;

/**
 * A Kafka producer application that produces {@link NextEvent} events.
 * <p>
 * The NexmarkProducer class encapsulates the logic for producing events to Kafka using a Kafka producer.
 * It leverages Kafka's features such as idempotence and transactions to ensure reliable and consistent event delivery.
 */
public class NexmarkKafkaProducer implements KafkaProducerApplication<NextEvent> {

    private static final String PRODUCER_CLIENT_ID = "nexmark-producer-client-id";
    private static final String KEY_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.LongSerializer";
    private static final String VALUE_SERIALIZER_CLASS = JacksonSerializer.class.getName();
    /**
     * The TransactionalId to use for transactional delivery. This enables reliability semantics which span multiple
     * producer sessions since it allows the client to guarantee that transactions using the same TransactionalId
     * have been completed prior to starting any new transactions. If no TransactionalId is provided, then the
     * producer is limited to idempotent delivery. If a TransactionalId is configured,
     * <code>enable.idempotence</code> is implied. By default, the TransactionId is not configured,
     * which means transactions cannot be used. Note that, by default, transactions require a
     * cluster of at least three brokers which is the recommended setting for production;
     * for development you can change this, by adjusting broker setting
     * <code>transaction.state.log.replication.factor</code>.;
     */
    private static final String TRANSACTIONAL_ID = UUID.randomUUID().toString();
    /**
     * In a Kafka producer, when you initiate a transaction using beginTransaction(), you have a
     * certain duration within which the transaction should be completed. If the transaction is not
     * completed within this specified timeout period, it will be considered as a failure.
     * The TRANSACTION_TIMEOUT_CONFIG property is being set to "60_000", which means the transaction timeout is
     * set to 60 seconds. This means that if the producer doesn't commit or abort the
     * transaction within 60 seconds after beginning it, the transaction will be considered timed out.
     * Setting an appropriate transaction timeout is important to balance between transaction completion time
     * and the risk of transaction failures. If your transactions involve complex processing or
     * operations that might take longer, you might need to adjust the timeout accordingly to avoid
     * premature transaction timeouts. It's worth noting that the default transaction timeout value
     * is usually higher (e.g., 5 minutes) unless explicitly configured.
     */
    private static final String TRANSACTION_TIMEOUT = "900000"; //"240000";
    /**
     * An idempotent producer has a unique producer ID and uses sequence IDs for each message,
     * allowing the broker to ensure, on a per-partition basis,
     * that it is committing ordered messages with no duplication
     * when set to true, it enables an idempotent producer which ensures
     * that exactly one copy of each message is written to the brokers, and in order.
     * The default value is enable.idempotence=false, so you must explicitly set this to
     * enable.idempotence=true
     * Once you enable the transactional.id, it is automatically implied that
     * enable.idempotence=true, so you do not need to explicitly set it. But, for clarity,
     * in this case we explicitly set it to true.
     */
    private static final String ENABLE_IDEMPOTENCE = "true";
    /**
     * The acks config controls the criteria under which requests are considered complete.
     * The "all" setting we have specified will result in blocking on the full commit of the record,
     * the slowest but most durable setting.
     * When set to "all" the producer will ensure that all in-sync replicas have received the record.
     * This is the strongest available guarantee.
     * The KafkaProducer uses the acks configuration to tell the leader broker how
     * many acknowledgments to wait for to consider a produce request complete. This
     * value must be acks=all for the idempotent producer to work, otherwise the producer
     * cannot guarantee idempotence. The default value is acks=1, so you have two choices:
     * <li> do not explicitly set it in the configuration and allow the producer automatically override it
     * <li> explicitly set this to acks=all. <br>
     * <br>
     * The producer will fail to start if enable.idempotence=true and your
     * application configures this to anything but acks=all
     * Once you enable the transactional.id, it is automatically implied that
     * acks=all, so you do not need to explicitly set it. But, for clarity,
     * in this case we explicitly set it to all.
     */
    private static final String ACKS = "all";
    /**
     * Setting a value greater than zero will cause the producer to resend any record
     * whose send fails with a potentially transient error. The only requirement for idempotency
     * is that this is greater than zero. The default value is already retries=2147483647,
     * so no change is required for the idempotent produce
     */
    private static final String RETRIES = "2147483647";
    /**
     * the maximum number of unacknowledged requests the producer sends
     * on a single connection before blocking. The idempotent producer still maintains
     * message order even with pipelining (i.e., max.in.flight.requests.per.connection can be greater
     * than 1), and the maximum value supported with idempotency is 5. The default value
     * is already max.in.flight.requests.per.connection=5, so no change is required
     * for the idempotent producer.
     */
    private static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "5";
    /**
     * The properties of the producer.
     */
    private final Properties properties;
    /**
     * The producer used to send events to Kafka.
     */
    private final Producer<Long, NextEvent> producer;
    /**
     * The list of metadata returned from the events sent to Kafka.
     */
    private final List<Future<RecordMetadata>> metadataList;
    /**
     * The generator used to generate events.
     */
    private final NexmarkGenerator generator;
    /**
     * The configuration of the generator.
     */
    private final GeneratorConfig config;
    /**
     * The size of the transaction.
     */
    private final long transactionSize;
    /**
     * The bootstrap server in Kafka is the initial entry point for clients to discover and
     * connect to the Kafka brokers within a cluster. It provides the necessary metadata
     * for clients to establish connections, retrieve information about topics and partitions,
     * and perform produce and consume operations.
     */
    private final String bootstrapServer;

    public NexmarkKafkaProducer(final String bootstrapServer, final GeneratorConfig config, final long transactionSize) {
        this.bootstrapServer = bootstrapServer;

        this.config = config;
        this.generator = new NexmarkGenerator(config);

        this.properties = new Properties();
        this.properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        this.properties.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_CLIENT_ID);
        this.properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS);
        this.properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS);
        this.properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID);
        this.properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, TRANSACTION_TIMEOUT);
        this.properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ENABLE_IDEMPOTENCE);
        this.properties.put(ProducerConfig.ACKS_CONFIG, ACKS);
        this.properties.put(ProducerConfig.RETRIES_CONFIG, RETRIES);
        this.properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        this.producer = new KafkaProducer<>(properties);
        this.metadataList = new ArrayList<>();
        this.transactionSize = transactionSize;
    }


    /**
     * Start to create NextEvents and send them to the producer.
     * <p>
     * The generator is used to create the events. The events are sent to the producer.
     * The producer sends the events to the correct broker and topic. The metadata of the
     * events are stored in a list of futures. Finally, when all the events are sent,
     * the producer is closed.
     * <p>
     * From Kafka 0.11, the KafkaProducer supports two additional modes: the idempotent producer and the
     * transactional producer. The idempotent producer strengthens Kafka's delivery semantics from at least once to
     * exactly once delivery. In particular producer retries will no longer introduce duplicates. The transactional
     * producer allows an application to send messages to multiple partitions (and topics!) atomically.
     * <p>
     * To enable idempotence, to enable.idempotence configuration must be set to true. If set, the retries config
     * will default to Integer.MAX_VALUE and the acks config will default to all. There are no API changes for the
     * idempotent producer, so existing applications will not need to be modified to take advantage of this feature.
     * <p>
     * To take advantage of the idempotent producer, it is imperative to avoid application level re-sends since
     * these cannot be de-duplicated. As such, if an application enables idempotence, it is recommended to leave the
     * retries config unset, as it will be defaulted to Integer.MAX_VALUE. Additionally, if a send(ProducerRecord)
     * returns an error even with infinite retries (for instance if the message expires in the buffer before being sent),
     * then it is recommended to shut down the producer and check the contents of the last produced message to ensure
     * that it is not duplicated. Finally, the producer can only guarantee idempotence for messages sent within a
     * single session.
     * <p>
     * To use the transactional producer and the attendant APIs, you must set the transactional.id configuration
     * property. If the transactional.id is set, idempotence is automatically enabled along with the producer configs
     * which idempotence depends on. Further, topics which are included in transactions should be configured for
     * durability. In particular, the replication.factor should be at least 3, and the min.insync.replicas for
     * these topics should be set to 2. Finally, in order for transactional guarantees to be realized from end-to-end,
     * the consumers must be configured to read only committed messages as well.
     * <p>
     * The purpose of the transactional.id is to enable transaction recovery across multiple sessions of a single
     * producer instance. It would typically be derived from the shard identifier in a partitioned, stateful,
     * application. As such, it should be unique to each producer instance running within a partitioned application.
     * <p>
     * All the new transactional APIs are blocking and will throw exceptions on failure.
     * <p>
     * All messages sent between the beginTransaction() and commitTransaction() calls will be part of a single transaction.
     * When the transactional.id is specified, all messages sent by the producer must be part of a transaction.
     * <p>
     * The transactional producer uses exceptions to communicate error states. In particular, it is not required to
     * specify callbacks for producer.send() or to call .get() on the returned Future: a KafkaException would be thrown
     * if any of the producer.send() or transactional calls hit an irrecoverable error during a transaction.
     * <p>
     * By calling producer.abortTransaction() upon receiving a KafkaException we can ensure that any successful
     * writes are marked as aborted, hence keeping the transactional guarantees.
     * <p>
     * ProducerFencedException: this fatal exception indicates that another producer with the same transactional.id has been started.
     * It is only possible to have one producer instance with a transactional.id at any given time, and the latest
     * one to be started "fences" the previous instances so that they can no longer make transactional requests. When you
     * encounter this exception, you must close the producer instance.
     * <p>
     * OutOfOrderSequenceException: this exception indicates that the broker received an unexpected sequence number from
     * the producer, which means that data may have been lost. If the producer is configured for idempotence only
     * (i.e. if enable.idempotence is set and no transactional.id is configured), it is possible to continue sending
     * with the same producer instance, but doing so risks reordering of sent records. For transactional producers,
     * this is a fatal error, and you should close the producer.
     */
    public void start() {
        // called first and once to fence zombies and abort any pending transaction
        this.producer.initTransactions();
        while (this.generator.hasNext()) {
            try {
                this.producer.beginTransaction();
                for (int i = 0; i < this.transactionSize; i++) {
                    NextEvent event = this.generator.next();
                    Future<RecordMetadata> metadata = produce(event);
                    this.metadataList.add(metadata);
                }
                this.producer.commitTransaction();
            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException
                     | UnsupportedVersionException e) {
                // We can't recover from these exceptions, so our only option is to close the producer and exit.
                this.producer.close();
            } catch (KafkaException e) {
                // For all other exceptions, just abort the transaction and try again.
                this.producer.abortTransaction();
            }

        }
        this.producer.close();
    }

    /**
     * Produce an event to Kafka.
     * <p>
     * Process the event and send it to the correct broker and topic.
     * <p>
     * Notice that the {@code send()} method is asynchronous and returns as
     * soon as the provided record is placed in the buffer of records to be sent to the broker.
     * Once the broker acknowledges that the record has been appended to its log,
     * the broker completes the produce request, which the application receives as
     * {@code RecordMetadata}—information about the committed message.
     * <p>
     * produce() is asynchronous, all it does is enqueue the message on an internal queue
     * which is later (>= queue.buffering.max.ms) served by internal
     * threads and sent to the broker (if a leader is available, else wait some more).
     * What this means in practice is that if you do:
     * <li>producer.send(record1);</li>
     * <li>producer.send(record2);</li>
     * <li>producer.exit();</li>
     * <p>
     * Then most likely neither message 1 nor message 2 will have actually been sent to the broker,
     * much less acknowledged by it.
     * <p>
     * Adding flush() before exiting will make the client wait for any outstanding messages to be delivered to the broker
     * (and this will be around queue.buffering.max.ms, plus latency).
     * If you add flush() after each produce() call you are effectively implementing a
     * sync producer which you shouldn't, see
     * <a href="https://github.com/edenhill/librdkafka/wiki/FAQ#why-is-there-no-sync-produce-interface">here</a>.
     *
     * @param value the event to produce
     */
    @Override
    public Future<RecordMetadata> produce(final NextEvent value) {
        Objects.requireNonNull(value);
        String topic = NexmarkUtil.selectTopic(value);
        Long key = Long.valueOf(NexmarkUtil.selectKey(value));
        ProducerRecord<Long, NextEvent> record = new ProducerRecord<>(topic, key, value);
        // no callback is needed: a KafkaException would be thrown if any of the producer.send() or transactional calls
        // hit an irrecoverable error during a transaction
        // however, I can show the metadata of the event when the sending is correctly committed within a
        // callback function
        return this.producer.send(record);
    }

    /**
     * Shutdown the producer.
     */
    @Override
    public void shutdown() {
        this.producer.close();
    }

    /**
     * Get the properties of the producer.
     *
     * @return the properties of the producer
     */
    public Properties getProperties() {
        return new Properties(this.properties);
    }

    /**
     * Get the producer.
     *
     * @return the producer
     */
    public Producer<Long, NextEvent> getProducer() {
        return this.producer;
    }

    /**
     * Get the list of metadata of the events.
     *
     * @return the list of metadata of the events
     */
    public List<Future<RecordMetadata>> getMetadataList() {
        return new ArrayList<>(this.metadataList);
    }

    /**
     * Get the generator.
     *
     * @return the generator
     */
    public NexmarkGenerator getGenerator() {
        return this.generator.copy();
    }

    /**
     * Get the configuration of the generator.
     *
     * @return the configuration of the generator
     */
    public GeneratorConfig getConfig() {
        return this.config;
    }

    /**
     * Get the size of the transaction.
     *
     * @return the size of the transaction
     */
    public long getTransactionSize() {
        return this.transactionSize;
    }

    /**
     * Get the bootstrap server.
     *
     * @return the bootstrap server
     */
    public String getBootstrapServer() {
        return this.bootstrapServer;
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
        if (!(o instanceof NexmarkKafkaProducer)) {
            return false;
        }
        NexmarkKafkaProducer that = (NexmarkKafkaProducer) o;
        return Objects.equals(getProperties(), that.getProperties()) && Objects.equals(getProducer(),
                that.getProducer()) && Objects.equals(getMetadataList(), that.getMetadataList())
                && Objects.equals(getGenerator(), that.getGenerator()) && Objects.equals(getConfig(), that.getConfig());
    }

    /**
     * Generated hashCode method, used to generate the hash values of objects.
     *
     * @return an integer whose value represents the hash value of the input object
     */
    @Override
    public int hashCode() {
        return Objects.hash(getProperties(), getProducer(), getMetadataList(), getGenerator(), getConfig());
    }

    /**
     * Generated toString method, used to print object's internal value.
     *
     * @return the string related to the object.
     */
    @Override
    public String toString() {
        return "NexmarkProducer{"
                + "properties=" + properties
                + ", producer=" + producer
                + ", metadataList=" + metadataList
                + ", generator=" + generator
                + ", config=" + config
                + '}';
    }
}
