package it.agilelab.thesis.nexmark.generator.model;

import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.model.Auction;

import java.time.Instant;
import java.util.Random;

/**
 * Generates auctions.
 */
public final class AuctionGenerator {
    /**
     * Keep the number of categories small so the example queries will find results even with a small
     * batch of events.
     */
    private static final int NUM_CATEGORIES = 5;
    /**
     * Number of yet-to-be-created people and auction ids allowed. The use of these "leads" allows
     * you to generate a random auction ID that has a greater chance of being still valid and active in the system.
     */
    private static final int AUCTION_ID_LEAD = 10;
    /**
     * Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are 1 over these
     * values. The term 'hot' is referred to a seller/bidder/auction with a lot of entry.
     */
    private static final int HOT_SELLER_RATIO = 100;
    /**
     * Name's max length is 20, description's max length is 100.
     * We use these constants to generate random strings.
     */
    private static final int MAX_LENGTH_NAME = 20;
    private static final int MAX_LENGTH_DESC = 100;

    private AuctionGenerator() {
        throw new AssertionError("This class is not meant to be instantiated.");
    }

    /**
     * Generate and return a random auction with next available id.
     *
     * @param eventsCountSoFar the number of events generated so far
     * @param eventId          the next event id to be generated
     * @param random           the random generator
     * @param timestamp        the timestamp of the event
     * @param config           the generator configuration
     * @return the generated auction
     */
    public static Auction nextAuction(final long eventsCountSoFar,
                                      final long eventId,
                                      final Random random,
                                      final long timestamp,
                                      final GeneratorConfig config) {
        long auctionId = nextAuctionId(eventId, config);
        long sellerId = nextSellerId(eventId, random, config);
        long category = nextCategory(random);
        long initialBid = PriceGenerator.nextPrice(random);
        long expirationDate = nextExpirationDate(eventsCountSoFar, random, timestamp, config);
        String name = StringsGenerator.nextString(random, MAX_LENGTH_NAME);
        String desc = StringsGenerator.nextString(random, MAX_LENGTH_DESC);
        long reserve = nextReserve(random, initialBid);
        int currentSize = nextCurrentSize(name, desc);
        String extra = StringsGenerator.nextExtra(random, currentSize, config.getConfiguration().getAvgAuctionByteSize());
        return new Auction(
                auctionId,
                name,
                desc,
                initialBid,
                reserve,
                Instant.ofEpochMilli(timestamp),
                Instant.ofEpochMilli(expirationDate),
                sellerId,
                category,
                extra);
    }

    /**
     * Returns the last valid auction id (ignoring FIRST_AUCTION_ID). Will be the current auction id if
     * due to generate an auction.
     * <p>
     * The meaning is the same as {@code lastBase0PersonId} in {@link PersonGenerator}.
     *
     * @param config  the generator configuration
     * @param eventId the next event id to be generated
     * @return the last valid auction id
     */
    public static long lastBase0AuctionId(final GeneratorConfig config, final long eventId) {
        long epoch = eventId / config.getTotalProportion();
        long offset = eventId % config.getTotalProportion();
        if (offset < config.getPersonProportion()) {
            // About to generate a person.
            // Go back to the last auction in the last epoch.
            epoch--;
            offset = (long) config.getAuctionProportion() - 1;
        } else if (offset >= config.getPersonProportion() + config.getAuctionProportion()) {
            // About to generate a bid.
            // Go back to the last auction generated in this epoch.
            offset = (long) config.getAuctionProportion() - 1;
        } else {
            // About to generate an auction.
            offset -= config.getPersonProportion();
        }
        return epoch * config.getAuctionProportion() + offset;
    }

    /**
     * Returns a random auction id (base 0).
     * <p>
     * Randomly select an auction ID within the range of the auctions that may still be active,
     * with some additional IDs added to increase the likelihood of getting an active auction.
     * <p>
     * This helps to generate a realistic and consistent auction ID with the current
     * state of the auctions in the Nexmark system.
     *
     * @param nextEventId the next event id to be generated
     * @param config      the generator configuration
     * @return a random auction id
     */
    public static long nextBase0AuctionId(final long nextEventId,
                                          final Random random,
                                          final GeneratorConfig config) {

        // Choose a random auction for any of those which are likely to still be in flight,
        // plus a few 'leads'.
        // Note that ideally we'd track non-expired auctions exactly, but that state
        // is difficult to split.
        // Oldest auction which might still be in flight.
        long minAuction =
                Math.max(lastBase0AuctionId(config, nextEventId) - config.getConfiguration().getNumInFlightAuctions(), 0);
        // Newest auction which might still be in flight.
        long maxAuction = lastBase0AuctionId(config, nextEventId);
        return minAuction + LongGenerator.nextLong(random, maxAuction - minAuction + 1 + AUCTION_ID_LEAD);
    }

    /**
     * Return a random time delay, in milliseconds, for length of auctions.
     */
    private static long nextAuctionLengthMs(final long eventsCountSoFar,
                                            final Random random,
                                            final long timestamp,
                                            final GeneratorConfig config) {

        // What's our current event number?
        long currentEventNumber = config.nextAdjustedEventNumber(eventsCountSoFar);
        // How many events till we've generated numInFlightAuctions?
        long numEventsForAuctions =
                ((long) config.getConfiguration().getNumInFlightAuctions() * config.getTotalProportion())
                        / config.getAuctionProportion();
        // When will the auction numInFlightAuctions beyond now be generated?
        long futureAuction = config.timestampForEvent(currentEventNumber + numEventsForAuctions);

        // LoggerFactory.getLogger(AuctionGenerator.class).debug("*** auction will be for {}ms ({} events ahead) ***", futureAuction - timestamp, numEventsForAuctions);

        // Choose a length with average horizonMs.
        long horizonMs = futureAuction - timestamp;
        return 1L + LongGenerator.nextLong(random, Math.max(horizonMs * 2, 1L));
    }


    private static int nextCurrentSize(final String name, final String desc) {
        // Auction(id, itemName, description, initialBid, reserve, expires, seller, category)
        // size in bytes of the auction
        return Long.BYTES + name.length() + desc.length()
                + Long.BYTES + Long.BYTES + Long.BYTES + Long.BYTES + Long.BYTES;
    }

    private static long nextReserve(final Random random,
                                    final long initialBid) {
        // In the context of auctions, the reserve price is the minimum price the seller is willing to
        return initialBid + PriceGenerator.nextPrice(random);
    }

    private static long nextExpirationDate(final long eventsCountSoFar,
                                           final Random random,
                                           final long timestamp,
                                           final GeneratorConfig config) {
        return timestamp + nextAuctionLengthMs(eventsCountSoFar, random, timestamp, config);
    }

    private static long nextCategory(final Random random) {
        return GeneratorConfig.getFirstCategoryId() + random.nextInt(NUM_CATEGORIES);
    }

    /**
     * The logic within the feature ensures that seller IDs are assigned based on the probability of having
     * "hot" sellers and generates a random seller ID when it is not a "hot" seller.
     */
    private static long nextSellerId(final long eventId, final Random random, final GeneratorConfig config) {
        long sellerId;
        // Here P(auction will be for a hot seller) = 1 - 1/hotSellersRatio.
        if (random.nextInt(config.getConfiguration().getHotSellersRatio()) > 0) {
            // Choose the first person in the batch of last HOT_SELLER_RATIO people.
            sellerId = (PersonGenerator.lastBase0PersonId(config, eventId) / HOT_SELLER_RATIO) * HOT_SELLER_RATIO;
        } else {
            sellerId = PersonGenerator.nextBase0PersonId(eventId, random, config);
        }
        sellerId += GeneratorConfig.getFirstPersonId();
        return sellerId;
    }

    private static long nextAuctionId(final long eventId, final GeneratorConfig config) {
        return lastBase0AuctionId(config, eventId) + GeneratorConfig.getFirstAuctionId();
    }


}
