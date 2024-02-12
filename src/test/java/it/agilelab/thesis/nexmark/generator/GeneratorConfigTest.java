package it.agilelab.thesis.nexmark.generator;

import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GeneratorConfigTest {

    private static GeneratorConfig config;

    @BeforeAll
    static void setup() {
        config = new GeneratorConfig(new NexmarkConfiguration(),
                System.currentTimeMillis(), 1, 100, 1);
    }

    @Test
    void testSplitWithZeroSubConfigs() {
        assertThrows(IllegalArgumentException.class, () -> config.split(0));
    }

    @Test
    void testSplitWithNegativeSubConfigs() {
        assertThrows(IllegalArgumentException.class, () -> config.split(-1));
    }

    @Test
    void testSplitWithOneSubConfigRightSize() {
        assertEquals(1, config.split(1).size());
    }

    @Test
    void testSameConfigWithOnlyOneSplit() {
        assertEquals(config, config.split(1).get(0));
    }

    @Test
    void testSplitWithMultipleSubConfigs() {
        int splitNumber = 3;
        List<GeneratorConfig> subConfigs = config.split(splitNumber);
        assertEquals(splitNumber, subConfigs.size());
        IntStream.range(0, splitNumber).forEach(i -> {
            // 1 34 67 with 3 splits and 100 events
            assertEquals((config.getMaxEvents() / splitNumber * i) + 1, subConfigs.get(i).getFirstEventId());
            // if last split, add 1 to max events to account for rounding
            if (i == splitNumber - 1) {
                assertEquals(config.getMaxEvents() / splitNumber + 1, subConfigs.get(i).getMaxEvents());
            } else {
                assertEquals(config.getMaxEvents() / splitNumber, subConfigs.get(i).getMaxEvents());
            }
        });
    }

    @Test
    void testEstimatedBytesForEvents() {
        // 2 people, 6 auctions, 92 bids
        // 2 * 200 + 6 * 500 + 92 * 100 = 12_600
        int expectedBytes = 12_600;
        assertEquals(expectedBytes, config.estimatedBytesForEvents(100));
    }

    @Test
    void testNextEventNumber() {
        int numEvents = 100;
        assertEquals(config.getFirstEventNumber() + numEvents, config.nextEventNumber(numEvents));
    }

    @Test
    void testNextAdjustedEventNumberWithOrderedStream() {
        int numEvents = 132;
        assertEquals(config.getFirstEventNumber() + numEvents, config.nextAdjustedEventNumber(numEvents));
    }

    /*
    @Test
    void testNextAdjustedEventNumberWithUnorderedStream() {
        int numEvents = 132;
        config.getConfiguration().setOutOfOrderGroupSize(10);
        // 132 + 1 = 133 => base = 133 / 10 = 13 * 10 = 130 => offset = (133 * 953) % 10 = 9 => 130 + 9 = 139
        int expectedEventNumber = 139;
        assertEquals(expectedEventNumber, config.nextAdjustedEventNumber(numEvents));
    }
    */

    @Test
    void testNextEventNumberForWatermarkWithOrderedStream() {
        int numEvents = 100;
        assertEquals(config.getFirstEventNumber() + numEvents, config.nextEventNumberForWatermark(numEvents));
    }

    /*
    @Test
    void testNextEventNumberForWatermarkWithUnorderedStream() {
        int numEvents = 100;
        config.getConfiguration().setOutOfOrderGroupSize(22);
        // 100 + 1 = 101 => 101 / 22 * 22 = 88
        int expectedEventNumber = 88;
        assertEquals(expectedEventNumber, config.nextEventNumberForWatermark(numEvents));
    }
    */

    @Test
    void testTimestampForEventInOrder() {
        int eventNumber = 100;
        long actualDelays = (long) (config.getInterEventDelayUs()[0] * eventNumber / 1000L);
        long baseTime = System.currentTimeMillis();
        LoggerFactory.getLogger(GeneratorConfigTest.class).info("Expected: " + (baseTime + actualDelays));
        LoggerFactory.getLogger(GeneratorConfigTest.class).info("Actual: " + config.timestampForEvent(eventNumber));
        assertEquals(baseTime + actualDelays, config.timestampForEvent(eventNumber), 100);
    }

    @Test
    void testTimestampForMultipleEventInOrder() {
        int eventNumber = 100;
        int secondEventNumber = 150;
        // 10
        long absoluteActualDelay = (long) (config.getInterEventDelayUs()[0] * eventNumber / 1000L);
        // 15
        long absoluteSecondActualDelay = (long) (config.getInterEventDelayUs()[0] * secondEventNumber / 1000L);
        long relativeActualDelay = config.getBaseTime() + absoluteActualDelay;
        long relativeSecondActualDelay = config.getBaseTime() + absoluteSecondActualDelay;
        int expectedDelay = (int) (relativeSecondActualDelay - relativeActualDelay);
        assertEquals(expectedDelay, config.timestampForEvent(secondEventNumber) - config.timestampForEvent(eventNumber));
    }

    // TODO: add more tests, varying all the variables in the config
}
