package it.agilelab.thesis.nexmark.generator.model;

import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.model.Bid;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

class BidGeneratorTest {
    private Random random;
    private GeneratorConfig config;

    @BeforeEach
    void setup() {
        random = new Random();
        NexmarkConfiguration nexmarkConfiguration = new NexmarkConfiguration();
        config = new GeneratorConfig(nexmarkConfiguration, System.currentTimeMillis(),
                1, 100, 1);
    }

    @Test
    void nextBidTest() {
        long eventId = 1;
        long adjustedEventTimeStamp = config.timestampForEvent(config.nextAdjustedEventNumber(0));
        Bid bid = BidGenerator.nextBid(eventId, random, adjustedEventTimeStamp, config);
        // Because we can have
        int maxAuctionId = 1011;
        assertTrue(bid.getAuction() <= maxAuctionId);
    }

    @Test
    void nextBidCheckAuctionTest() {
        int eventId = 341;
        long adjustedEventTimeStamp = config.timestampForEvent(config.nextAdjustedEventNumber(340));

        // The auctionId could be at most 1000 + (20 - 0 + 1 + 10) = 1031 (if the process entering in the else branch)
        // Otherwise it could be at most (20 / 100) -> 0,2 -> 0 * 100 -> 0 + 1000 = 1000
        long maxAuctionId = 1031;
        long minAuctionId = 1000;
        Bid bid = BidGenerator.nextBid(eventId, random, adjustedEventTimeStamp, config);
        assertTrue(minAuctionId <= bid.getAuction() && bid.getAuction() <= maxAuctionId);
    }

    @Test
    void nextBidCheckBidderTest() {
        int eventId = 341;
        long adjustedEventTimeStamp = config.timestampForEvent(config.nextAdjustedEventNumber(340));

        // When there is a hot bidder the minimum bidder id (at the moment) is epoch = 6, offset = 41 --> 1 - 1 = 0
        // thus, from lastBase0PersonId we collect 6. Then, 6 / 100 = 0,6 -> 0 * 100 = 0 + 1000 = 1000 + 1 = 1001.
        // But on the other hand, the maximum bidder id is 1016. Because: numPeople = 7, activePeople = 7, the random number
        // n is between 0 and 17 (not included). So the base 0 id value could be at most 7 - 7 + 16 = 16. Then
        // 1000 + 16 = 1016. So the minimum bidder id is 1000 and the maximum bidder id is 1016.
        long maxBidderId = 1016;
        long minBidderId = 1000;
        Bid bid = BidGenerator.nextBid(eventId, random, adjustedEventTimeStamp, config);
        assertTrue(minBidderId <= bid.getBidder() && bid.getBidder() <= maxBidderId);
    }

}
