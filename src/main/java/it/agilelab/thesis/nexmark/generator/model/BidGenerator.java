package it.agilelab.thesis.nexmark.generator.model;

import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.model.Bid;
import it.agilelab.thesis.nexmark.model.Pair;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static it.agilelab.thesis.nexmark.generator.model.StringsGenerator.nextString;

/**
 * Generates bids.
 */
public final class BidGenerator {
    /**
     * Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are 1 over these
     * values.
     */
    private static final int HOT_AUCTION_RATIO = 100;
    private static final int HOT_BIDDER_RATIO = 100;
    private static final int HOT_CHANNELS_RATIO = 2;
    private static final int CHANNELS_NUMBER = 10_000;
    private static final int MAX_URL_PART_LENGTH = 5;

    private static final String[] HOT_CHANNELS = new String[]{"Google", "Facebook", "Baidu", "Apple"};
    private static final String[] HOT_URLS = new String[]{getBaseUrl(new Random()), getBaseUrl(new Random()), getBaseUrl(new Random()), getBaseUrl(new Random())};
    private static final ConcurrentHashMap<Integer, Pair<String, String>> CHANNEL_URL_CACHE = new ConcurrentHashMap<>(CHANNELS_NUMBER);

    private BidGenerator() {
        throw new AssertionError("Private constructor");
    }

    /**
     * Generate and return a random bid with next available id.
     *
     * @param eventId   event id
     * @param random    random generator
     * @param timestamp timestamp of the bid
     * @param config    generator configuration
     * @return a random bid
     */
    public static Bid nextBid(final long eventId,
                              final Random random,
                              final long timestamp,
                              final GeneratorConfig config) {

        long auction = getAuctionId(eventId, random, config);

        long bidder = getBidderId(eventId, random, config);

        long price = PriceGenerator.nextPrice(random);

        Pair<String, String> resultChannelAndUrl = getResultForNextChannelAndUrl(random);
        String channel = resultChannelAndUrl.getFirst();
        String url = resultChannelAndUrl.getSecond();

        /*is this necessary? Check if it is used somewhere.
         Probably a bug: see https://github.com/nexmark/nexmark/issues/30
         This is a bug --> bidder += GeneratorConfig.getFirstPersonId(); I've checked also here:
         https://github.com/apache/beam/blob/master/sdks/java/testing/nexmark/src/main/java/org/apache/beam/sdk/nexmark/sources/generator/model/BidGenerator.java
         */

        int currentSize = getCurrentSize();
        String extra = StringsGenerator.nextExtra(random, currentSize, config.getConfiguration().getAvgBidByteSize());
        return new Bid(auction, bidder, price, channel, url, Instant.ofEpochMilli(timestamp), extra);
    }

    private static long getBidderId(final long eventId, final Random random, final GeneratorConfig config) {
        long bidder;
        // Here P(bid will be by a hot bidder) = 1 - 1/hotBiddersRatio
        if (random.nextInt(config.getConfiguration().getHotBiddersRatio()) > 0) {
            // Choose the second person (so hot bidders and hot sellers don't collide) in the batch of
            // last HOT_BIDDER_RATIO people.
            bidder = (PersonGenerator.lastBase0PersonId(config, eventId) / HOT_BIDDER_RATIO) * HOT_BIDDER_RATIO + 1;
        } else {
            bidder = PersonGenerator.nextBase0PersonId(eventId, random, config);
        }
        bidder += GeneratorConfig.getFirstPersonId();
        return bidder;
    }

    private static long getAuctionId(final long eventId, final Random random, final GeneratorConfig config) {
        long auction;
        // Here P(bid will be for a hot auction) = 1 - 1/hotAuctionRatio.
        if (random.nextInt(config.getConfiguration().getHotAuctionsRatio()) > 0) {
            // Choose the first auction in the batch of last HOT_AUCTION_RATIO auctions.
            long lastBase0AuctionId = AuctionGenerator.lastBase0AuctionId(config, eventId);
            auction = (lastBase0AuctionId / HOT_AUCTION_RATIO) * HOT_AUCTION_RATIO;
        } else {
            auction = AuctionGenerator.nextBase0AuctionId(eventId, random, config);
        }
        auction += GeneratorConfig.getFirstAuctionId();
        return auction;
    }

    private static String getBaseUrl(final Random random) {
        return "https://www.nexmark.com/"
                + nextString(random, MAX_URL_PART_LENGTH, '_') + '/'
                + nextString(random, MAX_URL_PART_LENGTH, '_') + '/'
                + nextString(random, MAX_URL_PART_LENGTH, '_') + '/'
                + "item.htm?query=1";
    }

    private static Pair<String, String> getResultForNextChannelAndUrl(final Random random) {
        if (random.nextInt(HOT_CHANNELS_RATIO) > 0) {
            int i = random.nextInt(HOT_CHANNELS.length);
            return new Pair<>(HOT_CHANNELS[i], HOT_URLS[i]);
        } else {
            return getNextChannelAndUrl(random.nextInt(CHANNELS_NUMBER), random);
        }
    }

    private static Pair<String, String> getNextChannelAndUrl(final int channelNumber, final Random random) {
        Pair<String, String> previousValue = CHANNEL_URL_CACHE.get(channelNumber);
        if (previousValue == null) {
            String url = getBaseUrl(random);
            if (random.nextInt(10) > 0) {
                url = url + "&channel_id=" + Math.abs(Integer.reverse(channelNumber));
            }
            CHANNEL_URL_CACHE.putIfAbsent(channelNumber, new Pair<>("channel-" + channelNumber, url));
        }

        return CHANNEL_URL_CACHE.get(channelNumber);
    }

    private static int getCurrentSize() {
        // auction + bidder + price + entryTime
        return Long.BYTES + Long.BYTES + Long.BYTES + Long.BYTES;
    }
}
