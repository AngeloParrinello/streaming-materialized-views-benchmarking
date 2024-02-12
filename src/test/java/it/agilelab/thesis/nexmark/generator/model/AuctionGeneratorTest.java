package it.agilelab.thesis.nexmark.generator.model;

import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.model.Auction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuctionGeneratorTest {

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
    void testFirstNextAuction() {
        long adjustedEventTimestamp = config.timestampForEvent(config.nextAdjustedEventNumber(0));
        Auction auction = AuctionGenerator.nextAuction(0, config.getFirstEventId(), random, adjustedEventTimestamp, config);
        LoggerFactory.getLogger(AuctionGeneratorTest.class).info("The generated auction is: " + auction);
        assertEquals(GeneratorConfig.getFirstAuctionId(), auction.getId());
    }

    @Test
    void testLastBase0AuctionId() {
        // Not really relevant for this test, but I needed to see it to understand the test itself
        for (int i = 1; i < 100; i++) {
            System.out.println("The event id is: " + i);
            long epoch = i / config.getTotalProportion();
            long offset = i % config.getTotalProportion();
            System.out.println("The epoch is: " + epoch);
            System.out.println("The offset is: " + offset);
            long total;
            if (offset < config.getPersonProportion()) {
                // About to generate a person.
                // Go back to the last auction in the last epoch.
                epoch--;
                offset = (long) config.getAuctionProportion() - 1;
                System.out.println("Generating a person, we don't want this id. Indeed, the offset is: " + offset);
            } else if (offset >= config.getPersonProportion() + config.getAuctionProportion()) {
                // About to generate a bid.
                // Go back to the last auction generated in this epoch.
                offset = (long) config.getAuctionProportion() - 1;
                System.out.println("Generating a bid, we don't want this id. Indeed, the offset is: " + offset);
            } else {
                // About to generate an auction.
                offset -= config.getPersonProportion();
                System.out.println("Generating an auction, we want this id. Indeed, the offset is: " + offset);
            }
            total = epoch * config.getAuctionProportion() + offset;
            System.out.println("The total is: " + total);
            System.out.println("---------------------------------------------");
        }
        int eventId = 301;
        // epoch = 6 (300 / 50) // offset = 1 (300 % 50) --> offset higher tan person proportion,
        // so we are generating an auction --> offset = 1 - 1 = 0 --> total = 6 * 3 + 0 = 18
        long expectedLastAuctionId = 18;
        long lastBase0AuctionId = AuctionGenerator.lastBase0AuctionId(config, eventId);
        assertEquals(expectedLastAuctionId, lastBase0AuctionId);
    }

    @Test
    void testNextBase0AuctionId() {
        int eventId = 301;
        // the next base 0 auction id should be lower than 29
        // Because:
        // lastBase0AuctionId(config, nextEventId) - config.getConfiguration().getNumInFlightAuctions() = -82
        // minAuction = Math.max(-82, 0); --> 0
        // maxAuction = lastBase0AuctionId(config, nextEventId) --> 18
        // the random base id will be: minAuction + LongGenerator.nextLong(random, maxAuction - minAuction + 1 + AUCTION_ID_LEAD);
        // --> 0 + LongGenerator.nextLong(random, 18 - 0 + 1 + 10) --> 0 + 29 = 29
        long idWillBeLowerThan = 29;
        long nextBase0AuctionId = AuctionGenerator.nextBase0AuctionId(eventId, random, config);
        LoggerFactory.getLogger(AuctionGeneratorTest.class).info("The next base 0 auction id is: " + nextBase0AuctionId);
        assertTrue(nextBase0AuctionId < idWillBeLowerThan);
    }

    @Test
    void testNextAuction() {
        int eventId = 321;
        long adjustedEventTimestamp = config.timestampForEvent(config.nextAdjustedEventNumber(eventId));
        Auction auction = AuctionGenerator.nextAuction(320, eventId, random, adjustedEventTimestamp, config);
        System.out.println("The generated auction is: " + auction);
        // 20 + 1000 = 1020
        long expectedId = 1020;
        assertEquals(expectedId, auction.getId());
    }
}
