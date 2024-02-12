package it.agilelab.thesis.nexmark.generator;

import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Parameters controlling how {@link NexmarkGenerator} synthesizes {@link it.agilelab.thesis.nexmark.model.Event} elements.
 */
public class GeneratorConfig implements Serializable {
    public static final long serialVersionUID = 2405172041950251807L;
    /**
     * We start the ids at specific values to help ensure the queries find a match even on small
     * synthesized dataset sizes.
     */
    private static final long FIRST_AUCTION_ID = 1000L;
    private static final long FIRST_PERSON_ID = 1000L;
    private static final long FIRST_CATEGORY_ID = 10L;

    /**
     * Proportions of people/auctions/bids to synthesize.
     */
    private final int personProportion;
    private final int auctionProportion;
    private final int bidProportion;
    private final int totalProportion;

    /**
     * Environment options.
     */
    private final NexmarkConfiguration configuration;

    /**
     * Delay between events, in microseconds. If the array has more than one entry then the rate is
     * changed every {@link #stepLengthSec}, and wraps around.
     */
    private final double[] interEventDelayUs;

    /**
     * Delay before changing the current inter-event delay.
     */
    private final long stepLengthSec;

    /**
     * Time for first event (ms since epoch).
     */
    private final long baseTime;

    /**
     * Event id of first event to be generated. Event ids are unique over all generators, and are used
     * as a seed to generate each event's data.
     */
    private final long firstEventId;

    /**
     * Maximum number of events to generate.
     */
    private final long maxEvents;

    /**
     * First event number. Generators running in parallel time may share the same event number, and
     * the event number is used to determine the event timestamp.
     */
    private final long firstEventNumber;

    /**
     * True period of epoch in milliseconds. Derived from above. (Ie time to run through cycle for all
     * interEventDelayUs entries).
     */
    private final long epochPeriodMs;

    /**
     * Number of events per epoch. Derived from above. (Ie number of events to run through cycle for
     * all interEventDelayUs entries).
     */
    private final long eventsPerEpoch;

    /**
     * Create a new (the default one) configuration based on NexmarkDefaultConfiguration.
     *
     * @param configuration    the NexmarkDefaultConfiguration to use
     * @param baseTime         the base time to use (usually System.currentTimeMillis(), system's time)
     * @param firstEventId     the first event id to use (usually 0 or 1)
     * @param maxEventsOrZero  the maximum number of events to generate (0 for no limit)
     * @param firstEventNumber the first event number to use (usually 0 or 1)
     */
    public GeneratorConfig(
            @NotNull final NexmarkConfiguration configuration,
            final long baseTime,
            final long firstEventId,
            final long maxEventsOrZero,
            final long firstEventNumber) {

        this.auctionProportion = configuration.getAuctionProp();
        this.personProportion = configuration.getPersonProp();
        this.bidProportion = configuration.getBidProportion();
        this.totalProportion = this.auctionProportion + this.personProportion + this.bidProportion;

        this.configuration = configuration;

        this.interEventDelayUs = new double[1];
        // Maybe a repetition ... further check needed
        // almost the same calculation as in RateShape
        this.interEventDelayUs[0] = ((double) RateUnit.PER_SECOND.getUsPerUnit())
                / configuration.getFirstEventRate() * configuration.getNumEventGenerators();

        this.stepLengthSec = configuration.getRateShape().stepLengthSec(configuration.getRatePeriodSec());
        this.baseTime = baseTime;
        this.firstEventId = firstEventId;

        if (maxEventsOrZero == 0) {
            // Scale maximum down to avoid overflow in getEstimatedSizeBytes.
            this.maxEvents =
                    Long.MAX_VALUE / ((long) this.totalProportion * Math.max(Math.max(configuration.getAvgPersonByteSize(),
                            configuration.getAvgAuctionByteSize()), configuration.getAvgBidByteSize()));
            LoggerFactory.getLogger(GeneratorConfig.class).warn(
                    "No limit on number of events generated, setting maxEvents to {}",
                    this.maxEvents);
        } else {
            this.maxEvents = maxEventsOrZero;
        }

        this.firstEventNumber = firstEventNumber;
        this.eventsPerEpoch = 0;
        this.epochPeriodMs = 0;
    }

    /**
     * Get the first auction id.
     *
     * @return the first auction id
     */
    public static long getFirstAuctionId() {
        return FIRST_AUCTION_ID;
    }

    /**
     * Get the first person id.
     *
     * @return the first person id
     */
    public static long getFirstPersonId() {
        return FIRST_PERSON_ID;
    }

    /**
     * Get the first category id.
     *
     * @return the first category id
     */
    public static long getFirstCategoryId() {
        return FIRST_CATEGORY_ID;
    }

    /**
     * Returns a copy of this config.
     *
     * @return a copy of this config
     */
    public GeneratorConfig copy() {
        return new GeneratorConfig(this.configuration, this.baseTime,
                this.firstEventId, this.maxEvents, this.firstEventNumber);
    }

    /**
     * Split this config into {@code n} sub-configs with roughly equal number of possible events, but
     * distinct value spaces. The generators will run on parallel timelines. This config should no
     * longer be used.
     *
     * @param n the number of sub-configs to create
     * @return a list of {@code n} sub-configs
     */
    public List<GeneratorConfig> split(final int n) {
        if (n < 1) {
            throw new IllegalArgumentException("n must be greater than 1");
        }

        List<GeneratorConfig> results = new ArrayList<>();
        if (n == 1) {
            // No split required.
            results.add(this);
        } else {
            long subMaxEvents = this.maxEvents / n;
            long subFirstEventId = this.firstEventId;
            for (int i = 0; i < n; i++) {
                if (i == n - 1) {
                    // To the last round of the cycle
                    // performed a check to make sure that no events are lost by rounding down.
                    subMaxEvents = this.maxEvents - subMaxEvents * (n - 1);
                }
                results.add(copyWith(subFirstEventId, subMaxEvents, this.firstEventNumber));
                subFirstEventId += subMaxEvents;
            }
        }
        return results;
    }

    /**
     * Return a copy of this config except with given parameters.
     *
     * @param firstEventId     the first event id to use
     * @param maxEvents        the maximum number of events to generate
     * @param firstEventNumber the first event number to use
     * @return a copy of this config except with given parameters
     */
    public GeneratorConfig copyWith(final long firstEventId, final long maxEvents, final long firstEventNumber) {
        return new GeneratorConfig(
                this.configuration, this.baseTime, firstEventId, maxEvents, firstEventNumber);
    }

    /**
     * Return an estimate of the bytes needed by {@code numEvents}.
     *
     * @param numEvents the number of events to estimate bytes for
     * @return an estimate of the bytes needed by {@code numEvents}
     */
    public long estimatedBytesForEvents(final long numEvents) {
        return getPeopleNumber(numEvents) * this.configuration.getAvgPersonByteSize()
                + getAuctionsNumber(numEvents) * this.configuration.getAvgAuctionByteSize()
                + getBidsNumber(numEvents) * this.configuration.getAvgBidByteSize();
    }

    private long getBidsNumber(final long numEvents) {
        return (numEvents * this.bidProportion) / this.totalProportion;
    }

    private long getAuctionsNumber(final long numEvents) {
        return (numEvents * this.auctionProportion) / this.totalProportion;
    }

    private long getPeopleNumber(final long numEvents) {
        return (numEvents * this.personProportion) / this.totalProportion;
    }

    /**
     * Return an estimate of the byte-size of all events a generator for this config would yield.
     *
     * @return an estimate of the byte-size of all events a generator for this config would yield
     */
    public long getEstimatedSizeBytes() {
        return estimatedBytesForEvents(this.maxEvents);
    }

    /**
     * Return the first 'event id' which could be generated from this config. Though events don't have
     * ids we can simulate them to help bookkeeping.
     *
     * @return the first 'event id' which could be generated from this config.
     */
    public long getStartEventId() {
        return this.firstEventId + this.firstEventNumber;
    }

    /**
     * Return one past the last 'event id' which could be generated from this config.
     *
     * @return one past the last 'event id' which could be generated from this config.
     */
    public long getStopEventId() {
        return this.firstEventId + this.firstEventNumber + this.maxEvents;
    }

    /**
     * Return the next event number for a generator which has so far emitted {@code numEvents}.
     *
     * @param numEvents the number of events emitted so far from a generator
     * @return the next event number
     */
    public long nextEventNumber(final long numEvents) {
        return this.firstEventNumber + numEvents;
    }

    /**
     * Return the next event number for a generator which has so far emitted {@code numEvents}, but
     * adjusted to account for {@code outOfOrderGroupSize}.
     * <p>
     * For this reason, we need to calculate the base value for event's offset is calculated.
     * This is done by dividing eventNumber by n, rounding down (in other methods we rounded up),
     * and multiplying the result by n. The resulting value represents the largest multiple of n less
     * than or equal to eventNumber. I.e. If n = 640 and numEvents = 2000, then base = 1920.
     * <p>
     * The offset is calculated by multiplying eventNumber by a constant and prime number (953 in this case). The
     * result is then divided by n and the remainder is taken. This is done to ensure that the offset is
     * always between 0 and n-1.
     * <p>
     * This method ensures that the next event number is adjusted to the size of the out-of-order event group.
     *
     * @param numEvents the number of events emitted so far from a generator
     * @return the next event number, that could be out of order
     */
    public long nextAdjustedEventNumber(final long numEvents) {
        long constant = 953;
        long n = this.configuration.getOutOfOrderGroupSize();
        long eventNumber = nextEventNumber(numEvents);
        long base = (eventNumber / n) * n;
        long offset = (eventNumber * constant) % n;
        return base + offset;
    }

    /**
     * Return the event number whose event time will be a suitable watermark for a generator which has
     * so far emitted {@code numEvents}.
     *
     * @param numEvents the number of events emitted so far from a generator
     * @return the number of the event
     */
    public long nextEventNumberForWatermark(final long numEvents) {
        long n = this.configuration.getOutOfOrderGroupSize();
        long eventNumber = nextEventNumber(numEvents);
        return (eventNumber / n) * n;
    }

    /**
     * What timestamp should the event with {@code eventNumber} have for this generator?
     * Returns the timestamp for an event (identified by {@code #eventNumber}) in milliseconds.
     *
     * @param eventNumber the number of the event
     * @return the timestamp for the event
     */
    public long timestampForEvent(final long eventNumber) {
        // we divide by 1000L to convert microseconds to milliseconds
        return this.baseTime + (long) (eventNumber * this.interEventDelayUs[0]) / 1000L;
    }

    /**
     * Get the person proportion.
     *
     * @return the person proportion
     */
    public int getPersonProportion() {
        return this.personProportion;
    }

    /**
     * Get the auction proportion.
     *
     * @return the auction proportion
     */
    public int getAuctionProportion() {
        return this.auctionProportion;
    }

    /**
     * Get the bid proportion.
     *
     * @return the bid proportion
     */
    public int getBidProportion() {
        return this.bidProportion;
    }

    /**
     * Get the total proportion.
     *
     * @return the total proportion
     */
    public int getTotalProportion() {
        return this.totalProportion;
    }

    /**
     * Get the configuration.
     *
     * @return the configuration
     */
    public NexmarkConfiguration getConfiguration() {
        return this.configuration;
    }

    /**
     * Get the base time.
     *
     * @return the base time
     */
    public long getBaseTime() {
        return this.baseTime;
    }

    /**
     * Get the inter event delay in us.
     *
     * @return the inter event delay in us
     */
    public double[] getInterEventDelayUs() {
        return Arrays.copyOf(this.interEventDelayUs, this.interEventDelayUs.length);
    }

    /**
     * Get the step length in sec.
     *
     * @return the step length in sec
     */
    public long getStepLengthSec() {
        return this.stepLengthSec;
    }

    /**
     * Get the first event id.
     *
     * @return the first event id
     */
    public long getFirstEventId() {
        return this.firstEventId;
    }

    /**
     * Get the max events.
     *
     * @return the max events
     */
    public long getMaxEvents() {
        return this.maxEvents;
    }

    /**
     * Get the first event number.
     *
     * @return the first event number
     */
    public long getFirstEventNumber() {
        return this.firstEventNumber;
    }

    /**
     * Get the epoch period in ms.
     *
     * @return the epoch period in ms
     */
    public long getEpochPeriodMs() {
        return this.epochPeriodMs;
    }

    /**
     * Get the events per epoch.
     *
     * @return the events per epoch
     */
    public long getEventsPerEpoch() {
        return this.eventsPerEpoch;
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
        if (!(o instanceof GeneratorConfig)) {
            return false;
        }
        GeneratorConfig that = (GeneratorConfig) o;
        return personProportion == that.personProportion && auctionProportion == that.auctionProportion && bidProportion == that.bidProportion && totalProportion == that.totalProportion && stepLengthSec == that.stepLengthSec && baseTime == that.baseTime && firstEventId == that.firstEventId && maxEvents == that.maxEvents && firstEventNumber == that.firstEventNumber && epochPeriodMs == that.epochPeriodMs && eventsPerEpoch == that.eventsPerEpoch && Objects.equals(configuration, that.configuration) && Arrays.equals(interEventDelayUs, that.interEventDelayUs);
    }

    /**
     * Generated hashCode method, used to generate the hash values of objects.
     *
     * @return an integer whose value represents the hash value of the input object
     */
    @Override
    public int hashCode() {
        int result = Objects.hash(personProportion, auctionProportion, bidProportion, totalProportion, configuration, stepLengthSec, baseTime, firstEventId, maxEvents, firstEventNumber, epochPeriodMs, eventsPerEpoch);
        result = 31 * result + Arrays.hashCode(interEventDelayUs);
        return result;
    }

    /**
     * Generated toString method, used to print object's internal value.
     *
     * @return the string related to the object.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("GeneratorConfig");
        sb.append("{configuration:");
        sb.append(configuration.toString());
        sb.append(";interEventDelayUs=[");
        for (int i = 0; i < interEventDelayUs.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(interEventDelayUs[i]);
        }
        sb.append("]");
        sb.append(";stepLengthSec:");
        sb.append(stepLengthSec);
        sb.append(";baseTime:");
        sb.append(baseTime);
        sb.append(";firstEventId:");
        sb.append(firstEventId);
        sb.append(";maxEvents:");
        sb.append(maxEvents);
        sb.append(";firstEventNumber:");
        sb.append(firstEventNumber);
        sb.append(";epochPeriodMs:");
        sb.append(epochPeriodMs);
        sb.append(";eventsPerEpoch:");
        sb.append(eventsPerEpoch);
        sb.append("}");
        return sb.toString();
    }
}
