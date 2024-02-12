package it.agilelab.thesis.nexmark.ksql;


import io.confluent.ksql.api.client.Row;
import io.confluent.shaded.org.reactivestreams.Subscriber;
import io.confluent.shaded.org.reactivestreams.Subscription;
import it.agilelab.thesis.nexmark.NexmarkUtil;
import org.apache.logging.log4j.Logger;

import java.time.Instant;

/**
 * To consume the data from the KsqlDB server asynchronously, we need to implement the {@link Subscriber} interface,
 * which can receive query result rows.
 * <p>
 * This {@link Subscriber} it is based on the (<a href="http://www.reactive-streams.org/">Reactive Streams</a>)
 * specification.
 */
public class NexmarkSubscriber implements Subscriber<Row> {
    private final Logger logger = NexmarkUtil.getLogger();
    /**
     * The subscription to the {@link org.reactivestreams.Publisher}.
     */
    private Subscription subscription;
    private Instant start;
    private Instant end;
    private int count;
    private boolean isRunning;


    public NexmarkSubscriber() {
    }


    /**
     * Method invoked prior to invoking any other Subscriber methods for the given Subscription.
     *
     * @param subscription the subscription from which data can be requested
     */
    @Override
    public synchronized void onSubscribe(final Subscription subscription) {
        this.logger.info("Subscribed to the query.");
        this.subscription = subscription;
        this.start = Instant.now();
        this.isRunning = true;
        // Request the first row
        this.subscription.request(1);
    }

    /**
     * Data notification sent by the {@link org.reactivestreams.Publisher} in response to requests to {@link Subscription#request(long)}.
     *
     * @param row the element signaled
     */
    @Override
    public synchronized void onNext(final Row row) {
        this.count++;
        this.logger.info("Received a row!");
        this.logger.info("Row: " + row.values());
        this.logger.info("Column names: " + row.columnNames());
        this.logger.info("Count: " + this.count);
        // Request the next row
        this.subscription.request(1);
    }

    /**
     * Failed terminal state.
     * <p>
     * No further events will be sent even if {@link Subscription#request(long)} is invoked again.
     *
     * @param t the throwable signaled
     */
    @Override
    public synchronized void onError(final Throwable t) {
        this.end = Instant.now();
        this.logger.info("Error occurred: " + t.getMessage());
    }

    /**
     * Successful terminal state.
     * <p>
     * No further events will be sent even if {@link Subscription#request(long)} is invoked again.
     */
    @Override
    public synchronized void onComplete() {
        this.end = Instant.now();
        this.isRunning = false;
        this.logger.info("Query has ended.");
    }

    /**
     * True if the query is still running, false otherwise.
     *
     * @return true if the query is still running, false otherwise.
     */
    public synchronized boolean isRunning() {
        return this.isRunning;
    }

    /**
     * Get the number of rows received.
     *
     * @return the number of rows received.
     */
    public synchronized int getCount() {
        return this.count;
    }

    /**
     * Get the start time of the query.
     *
     * @return the start time of the query.
     */
    public synchronized Instant getStart() {
        return this.start;
    }

    /**
     * Get the end time of the query.
     *
     * @return the end time of the query.
     */
    public synchronized Instant getEnd() {
        return this.end;
    }

    /**
     * Get the duration of the query. If the query is still running, the duration is {@link Instant#MIN}.
     *
     * @return the duration of the query.
     */
    public synchronized Instant getDuration() {
        if (isRunning) {
            return Instant.MIN;
        }
        return Instant.ofEpochMilli(this.end.toEpochMilli() - this.start.toEpochMilli());
    }
}
