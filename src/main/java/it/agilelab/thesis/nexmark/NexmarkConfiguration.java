package it.agilelab.thesis.nexmark;


import it.agilelab.thesis.nexmark.generator.RateShape;
import it.agilelab.thesis.nexmark.generator.RateUnit;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class NexmarkConfiguration implements Serializable {
    private static final long serialVersionUID = 1905122041950251207L;
    /**
     * Number of events to generate. If zero, generate as many as possible without overflowing
     * internal counters etc.
     */
    @JsonProperty("numEvents")
    private final long numEvents = 0;

    /**
     * Number of event generators to use. Each generates events in its own timeline.
     */
    @JsonProperty("numEventGenerators")
    private final int numEventGenerators = 1;

    /**
     * Shape of event rate curve.
     */
    @JsonProperty("rateShape")
    private final RateShape rateShape = RateShape.SQUARE;

    /**
     * Initial overall event rate (in {@link #rateUnit}).
     */
    @JsonProperty("firstEventRate")
    private final int firstEventRate = 10000;

    /**
     * Next overall event rate (in {@link #rateUnit}).
     */
    @JsonProperty("nextEventRate")
    private final int nextEventRate = 10000;

    /**
     * Unit for rates.
     */
    @JsonProperty("rateUnit")
    private final RateUnit rateUnit = RateUnit.PER_SECOND;

    /**
     * Overall period of rate shape, in seconds.
     */
    @JsonProperty("ratePeriodSec")
    private final int ratePeriodSec = 600;

    /**
     * Time in seconds to preload the subscription with data, at the initial input rate of the
     * pipeline.
     */
    @JsonProperty("preloadSeconds")
    private final int preloadSeconds = 0;

    /**
     * Timeout for stream pipelines to stop in seconds.
     */
    @JsonProperty("streamTimeout")
    private final int streamTimeout = 240;

    /**
     * If true, and in streaming mode, generate events only when they are due according to their
     * timestamp.
     */
    @JsonProperty("isRateLimited")
    private final boolean isRateLimited = false;

    /**
     * If true, use wallclock time as event time. Otherwise, use a deterministic time in the past so
     * that multiple runs will see exactly the same event streams and should thus have exactly the
     * same results.
     */
    @JsonProperty("useWallclockEventTime")
    private final boolean useWallclockEventTime = false;

    /**
     * Person Proportion. Between 0 and 100, the percentage of people to generate.
     * This means every 50 events there will be 1 person.
     */
    @JsonProperty("personProportion")
    private final int personProp = 1;

    /**
     * Auction Proportion. Between 0 and 100, the percentage of auctions to generate.
     * This means that every 33 events there will be 1 auction.
     */
    @JsonProperty("auctionProportion")
    private final int auctionProp = 3;

    /**
     * Bid Proportion. Between 0 and 100, the percentage of bids to generate.
     * This means that every 2 events there will be 1 bid.
     */
    @JsonProperty("bidProportion")
    private final int bidProp = 46;

    /**
     * Average idealized size of a 'new person' event, in bytes.
     */
    @JsonProperty("avgPersonByteSize")
    private final int avgPersonByteSize = 200;

    /**
     * Average idealized size of a 'new auction' event, in bytes.
     */
    @JsonProperty("avgAuctionByteSize")
    private final int avgAuctionByteSize = 500;

    /**
     * Average idealized size of a 'bid' event, in bytes.
     */
    @JsonProperty("avgBidByteSize")
    private final int avgBidByteSize = 100;

    /**
     * Ratio of bids to 'hot' auctions compared to all other auctions.
     */
    @JsonProperty("hotAuctionRatio")
    private final int hotAuctionsRatio = 2;

    /**
     * Ratio of auctions for 'hot' sellers compared to all other people.
     */
    @JsonProperty("hotSellersRatio")
    private final int hotSellersRatio = 4;

    /**
     * Ratio of bids for 'hot' bidders compared to all other people.
     */
    @JsonProperty("hotBiddersRatio")
    private final int hotBiddersRatio = 4;

    /**
     * Window size, in seconds, for queries 3, 5, 7 and 8.
     */
    @JsonProperty("windowSizeSec")
    private final long windowSizeSec = 10;

    /**
     * Sliding window period, in seconds, for query 5.
     */
    @JsonProperty("windowPeriodSec")
    private final long windowPeriodSec = 5;

    /**
     * Number of seconds to hold back events according to their reported timestamp.
     */
    @JsonProperty("watermarkHoldbackSec")
    private final long watermarkHoldbackSec = 0;

    /**
     * Average number of auction which should be inflight at any time, per generator.
     */
    @JsonProperty("numInFlightAuctions")
    private final int numInFlightAuctions = 100;

    /**
     * Maximum number of people to consider as active for placing auctions or bids.
     */
    @JsonProperty("maxNumActivePeople")
    private final int maxNumActivePeople = 1000;

    /**
     * Length of occasional delay to impose on events (in seconds).
     */
    @JsonProperty("occasionalDelaySec")
    private final long occasionalDelaySec = 3;

    /**
     * Probability that an event will be delayed by delayS.
     */
    @JsonProperty("probDelayedEvent")
    private final double probDelayedEvent = 0.1;

    /**
     * Number of events in out-of-order groups. 1 implies no out-of-order events. 1000 implies every
     * 1000 events per generator are emitted in pseudo-random order. Should be greater than 0.
     * <p>
     * Note that this is the number of events per generator, not the total number of events.
     * <p>
     * Indicates the number of consecutive events that will be issued in the correct order before
     * an out-of-order event group is entered. For example, if {@code outOfOrderGroupSize} is 10,
     * it means that 10 consecutive events will be issued in the correct order and only the next event
     * (the eleventh) will be issued out of order.
     * <p>
     * The higher the value, the more out-of-order events will be issued.
     */
    @JsonProperty("outOfOrderGroupSize")
    private final long outOfOrderGroupSize = 1;

    /**
     * Get the number of events to generate.
     *
     * @return the number of events to generate
     */
    public long getNumEvents() {
        return numEvents;
    }

    /**
     * Get the number of event generators.
     *
     * @return the number of event generators
     */
    public int getNumEventGenerators() {
        return numEventGenerators;
    }

    /**
     * Get the rate shape.
     *
     * @return the rate shape
     */
    public RateShape getRateShape() {
        return rateShape;
    }

    /**
     * Get the initial overall event rate.
     *
     * @return the initial overall event rate
     */
    public int getFirstEventRate() {
        return firstEventRate;
    }

    /**
     * Get the next overall event rate.
     *
     * @return the next overall event rate
     */
    public int getNextEventRate() {
        return nextEventRate;
    }


    /**
     * Get the rate unit.
     *
     * @return the rate unit
     */
    public RateUnit getRateUnit() {
        return rateUnit;
    }

    /**
     * Get the rate period in seconds.
     *
     * @return the rate period in seconds
     */
    public int getRatePeriodSec() {
        return ratePeriodSec;
    }

    /**
     * Get the time in seconds to preload the subscription with data, at the initial input rate of the
     * pipeline.
     *
     * @return the number of seconds to preload the subscription with data, at the initial input rate of the
     * pipeline
     */
    public int getPreloadSeconds() {
        return preloadSeconds;
    }

    /**
     * Get the number of seconds to run the pipeline before shutting it down.
     *
     * @return the number of seconds to run the pipeline before shutting it down
     */
    public int getStreamTimeout() {
        return streamTimeout;
    }

    /**
     * If true, and in streaming mode, generate events only when they are due according to their
     * timestamp.
     *
     * @return true if and only if we should generate events only when they are due according to their timestamp
     */
    public boolean getIsRateLimited() {
        return isRateLimited;
    }

    /**
     * Get if the wallclock time is used as event time.
     *
     * @return true if and only if the wallclock time is used as event time
     */
    public boolean isUseWallclockEventTime() {
        return useWallclockEventTime;
    }

    /**
     * Get the proportion of a person into the stream.
     *
     * @return the proportion of a person into the stream
     */
    public int getPersonProp() {
        return personProp;
    }

    /**
     * Get the proportion of an auction into the stream.
     *
     * @return the proportion of an auction into the stream
     */
    public int getAuctionProp() {
        return auctionProp;
    }

    /**
     * Get the proportion of a bid into the stream.
     *
     * @return the proportion of a bid into the stream
     */
    public int getBidProportion() {
        return bidProp;
    }

    /**
     * Get the average size of a person in bytes.
     *
     * @return the average size of a person in bytes
     */
    public int getAvgPersonByteSize() {
        return avgPersonByteSize;
    }

    /**
     * Get the average size of an auction in bytes.
     *
     * @return the average size of an auction in bytes
     */
    public int getAvgAuctionByteSize() {
        return avgAuctionByteSize;
    }

    /**
     * Get the average size of a bid in bytes.
     *
     * @return the average size of a bid in bytes
     */
    public int getAvgBidByteSize() {
        return avgBidByteSize;
    }


    /**
     * Get the ratio of hot auctions.
     *
     * @return the ratio of hot auctions
     */
    public int getHotAuctionsRatio() {
        return hotAuctionsRatio;
    }


    /**
     * Get the ratio of hot sellers.
     *
     * @return the ratio of hot sellers
     */
    public int getHotSellersRatio() {
        return hotSellersRatio;
    }

    /**
     * Get the ratio of hot bidders.
     *
     * @return the ratio of hot bidders
     */
    public int getHotBiddersRatio() {
        return hotBiddersRatio;
    }


    /**
     * Get the window size, in seconds, for queries 3, 5, 7 and 8.
     *
     * @return the window size, in seconds, for queries 3, 5, 7 and 8
     */
    public long getWindowSizeSec() {
        return windowSizeSec;
    }

    /**
     * Get the window period, in seconds, for query 5.
     *
     * @return the window period, in seconds, for query 5
     */
    public long getWindowPeriodSec() {
        return windowPeriodSec;
    }


    /**
     * Get the watermark holdback, in seconds.
     *
     * @return the watermark holdback, in seconds
     */
    public long getWatermarkHoldbackSec() {
        return watermarkHoldbackSec;
    }


    /**
     * Get the number of auctions in flight.
     *
     * @return the number of auctions in flight
     */
    public int getNumInFlightAuctions() {
        return numInFlightAuctions;
    }

    /**
     * Get the maximum number of active people.
     *
     * @return the maximum number of active people
     */
    public int getMaxNumActivePeople() {
        return maxNumActivePeople;
    }

    /**
     * Get the occasional delay, in seconds.
     *
     * @return the occasional delay, in seconds
     */
    public long getOccasionalDelaySec() {
        return occasionalDelaySec;
    }


    /**
     * Get the probability of a delayed event.
     *
     * @return the probability of a delayed event
     */
    public double getProbDelayedEvent() {
        return this.probDelayedEvent;
    }


    /**
     * Get the number of out-of-order events group.
     *
     * @return the number of out-of-order events group
     */
    public long getOutOfOrderGroupSize() {
        return this.outOfOrderGroupSize;
    }


    /**
     * Generated toString method, used to print object's internal value.
     *
     * @return the string related to the object.
     */
    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Generated hashCode method, used to generate the hash values of objects.
     *
     * @return an integer whose value represents the hash value of the input object
     */
    @Override
    public int hashCode() {
        return Objects.hash(
                rateShape,
                firstEventRate,
                nextEventRate,
                rateUnit,
                ratePeriodSec,
                preloadSeconds,
                streamTimeout,
                isRateLimited,
                useWallclockEventTime,
                personProp,
                auctionProp,
                bidProp,
                avgPersonByteSize,
                avgAuctionByteSize,
                avgBidByteSize,
                hotAuctionsRatio,
                hotSellersRatio,
                hotBiddersRatio,
                windowSizeSec,
                windowPeriodSec,
                watermarkHoldbackSec,
                numInFlightAuctions,
                maxNumActivePeople,
                occasionalDelaySec,
                probDelayedEvent,
                outOfOrderGroupSize);
    }

    /**
     * Generated equals method, used to compare objects.
     *
     * @param o the object to compare with
     * @return true if the input object is equal to the current object
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NexmarkConfiguration defaultConfiguration = (NexmarkConfiguration) o;
        return getNumEvents() == defaultConfiguration.getNumEvents()
                && getNumEventGenerators() == defaultConfiguration.getNumEventGenerators()
                && getFirstEventRate() == defaultConfiguration.getFirstEventRate()
                && getNextEventRate() == defaultConfiguration.getNextEventRate()
                && getRatePeriodSec() == defaultConfiguration.getRatePeriodSec()
                && getPreloadSeconds() == defaultConfiguration.getPreloadSeconds()
                && getStreamTimeout() == defaultConfiguration.getStreamTimeout()
                && getIsRateLimited() == defaultConfiguration.getIsRateLimited()
                && isUseWallclockEventTime() == defaultConfiguration.isUseWallclockEventTime()
                && getPersonProp() == defaultConfiguration.getPersonProp()
                && getAuctionProp() == defaultConfiguration.getAuctionProp()
                && getBidProportion() == defaultConfiguration.getBidProportion()
                && getAvgPersonByteSize() == defaultConfiguration.getAvgPersonByteSize()
                && getAvgAuctionByteSize() == defaultConfiguration.getAvgAuctionByteSize()
                && getAvgBidByteSize() == defaultConfiguration.getAvgBidByteSize()
                && getHotAuctionsRatio() == defaultConfiguration.getHotAuctionsRatio()
                && getHotSellersRatio() == defaultConfiguration.getHotSellersRatio()
                && getHotBiddersRatio() == defaultConfiguration.getHotBiddersRatio()
                && getWindowSizeSec() == defaultConfiguration.getWindowSizeSec()
                && getWindowPeriodSec() == defaultConfiguration.getWindowPeriodSec()
                && getWatermarkHoldbackSec() == defaultConfiguration.getWatermarkHoldbackSec()
                && getNumInFlightAuctions() == defaultConfiguration.getNumInFlightAuctions()
                && getMaxNumActivePeople() == defaultConfiguration.getMaxNumActivePeople()
                && getOccasionalDelaySec() == defaultConfiguration.getOccasionalDelaySec()
                && getOutOfOrderGroupSize() == defaultConfiguration.getOutOfOrderGroupSize()
                && getRateShape() == defaultConfiguration.getRateShape()
                && getRateUnit() == defaultConfiguration.getRateUnit()
                && Double.compare(defaultConfiguration.getProbDelayedEvent(), getProbDelayedEvent()) == 0;
    }
}
