package it.agilelab.thesis.nexmark.generator;

import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.model.Event;
import it.agilelab.thesis.nexmark.model.NextEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

class NexmarkGeneratorTest {

    private NexmarkGenerator generator;

    @BeforeEach
    void setup() {
        long firstEventId = 1;
        long maxEvents = 1000;
        long firstEventNumber = 0;
        long baseTime = System.currentTimeMillis();

        NexmarkConfiguration nexmarkConfiguration = new NexmarkConfiguration();

        GeneratorConfig config = new GeneratorConfig(nexmarkConfiguration, baseTime, firstEventId, maxEvents, firstEventNumber);

        generator = new NexmarkGenerator(config);
    }

    @Test
    void testDemoGenerate() {
        NexmarkConfiguration nexmarkConfiguration = new NexmarkConfiguration();
        GeneratorConfig generatorConfig = new GeneratorConfig(
                nexmarkConfiguration, System.currentTimeMillis(), 1, 100, 1);
        NexmarkGenerator generator = new NexmarkGenerator(generatorConfig);
        int count = 0;
        while (generator.hasNext()) {
            Event<?> event = generator.next().getActualEvent();
            count++;
            System.out.println(event);
        }
        assertEquals(100, count);
    }


    @Test
    void testCopyNexmarkGenerator() {
        NexmarkGenerator copy = generator.copy();
        assertEquals(generator.getCurrentConfig(), copy.getCurrentConfig());
        assertEquals(generator.getEventsCountSoFar(), copy.getEventsCountSoFar());
        assertEquals(generator.getWallclockBaseTime(), copy.getWallclockBaseTime());
        assertNotSame(generator, copy);
    }

    @Test
    void testHasNext() {
        assertTrue(generator.hasNext());
    }

    @Test
    void testNext() {
        assertNotNull(generator.next());
    }

    @Test
    void testNextWithNoNext() {
        NexmarkGenerator copy = generator.copy();
        while (copy.hasNext()) {
            copy.next();
        }
        assertThrows(NoSuchElementException.class, copy::next);
    }

    @Test
    void testRemoveUnsupported() {
        assertThrows(UnsupportedOperationException.class, generator::remove);
    }

    @Test
    void testSplitAtEventIdNewMaxEventId() {
        long splitEventId = 50;
        NexmarkGenerator copy = generator.copy();
        copy.splitAtEventId(splitEventId);
        int expectedNewMaxEventId = 49;
        assertEquals(expectedNewMaxEventId, copy.getCurrentConfig().getMaxEvents());
    }

    @Test
    void testSplitAtEventIdNewFirstEventId() {
        long splitEventId = 50;
        NexmarkGenerator copy = generator.copy();
        GeneratorConfig split = copy.splitAtEventId(splitEventId);
        // 1, as the previous event id
        int expectedNewFirstEventId = 1;
        assertEquals(expectedNewFirstEventId, split.getFirstEventId());
    }

    @Test
    void testSplitAtEventIdNewFirstEventNumber() {
        long splitEventId = 50;
        NexmarkGenerator copy = generator.copy();
        GeneratorConfig split = copy.splitAtEventId(splitEventId);
        // 0 (the initial event number) + 49 (the event id before the split event)
        int expectedNewFirstEventNumber = 49;
        assertEquals(expectedNewFirstEventNumber, split.getFirstEventNumber());
    }

    @Test
    void testWallClockBaseTimeInitiallyIsNotSet() {
        assertEquals(-1, generator.getWallclockBaseTime());
    }

    @Test
    void testWallClockBaseTimeIsSetAfterTheFirstNext() {
        long baseTime = System.currentTimeMillis();
        generator.next();
        assertEquals(baseTime, generator.getWallclockBaseTime(), 100);
    }

    @Test
    void testNextEventTimeStampOrderedStream() {
        NextEvent nextEvent = generator.next();
        for (int i = 0; i < 49; i++) {
            generator.next();
        }
        NextEvent nextEvent2 = generator.next();
        assertTrue(nextEvent.getEventTimestamp() < nextEvent2.getEventTimestamp());
        assertEquals(5, nextEvent2.getEventTimestamp() - nextEvent.getEventTimestamp());
    }

    /*
    @Test
    void testNextEventTimeStampOutOfOrderStream() {
        NexmarkGenerator copy = generator.copy();
        copy.getCurrentConfig().getConfiguration().setOutOfOrderGroupSize(78);
        for (int i = 0; i < 24; i++) {
            NextEvent event = copy.next();
            // As is possible see from the logs, the event id is not in order
            LoggerFactory.getLogger(NexmarkGeneratorTest.class).info(() -> "Event timestamp: " + event.getEventTimestamp());
        }
        // In an ordered stream, the event timestamp should be the same
        // As the difference is very small
        NextEvent event = copy.next();
        NextEvent event2 = copy.next();
        assertNotEquals(event.getEventTimestamp(), event2.getEventTimestamp());
    }

    @Test
    void testNextEventWatermarkOutOfOrderStream() {
        NexmarkGenerator copy = generator.copy();
        copy.getCurrentConfig().getConfiguration().setOutOfOrderGroupSize(78);
        GeneratorConfig config = copy.getCurrentConfig();
        config.getConfiguration().setNumEvents(200);
        copy = new NexmarkGenerator(config);
        // TODO : this should be 200 but it's 100: change how the generator take the configuration
        System.out.println(copy.getCurrentConfig().getMaxEvents());
        NextEvent nextEvent = copy.next();
        LoggerFactory.getLogger(NexmarkGeneratorTest.class).info(() -> "Watermark: " + nextEvent.getWatermark());

        // The watermark should be the same for the first 78 events
        for (int i = 0; i < 77; i++) {
            NextEvent event = copy.next();
            LoggerFactory.getLogger(NexmarkGeneratorTest.class).info(() -> "Watermark: " + event.getWatermark());
            assertEquals(nextEvent.getWatermark(), event.getWatermark());
        }
        NextEvent nextEvent2 = copy.next();
        assertTrue(nextEvent.getWatermark() < nextEvent2.getWatermark());
        // The watermark should be the same for the next 78 events
        // 7 eventIds have been produced in the meantime
        assertEquals(7, nextEvent2.getWatermark() - nextEvent.getWatermark());
        for (int i = 0; i < 77; i++) {
            NextEvent event = copy.next();
            LoggerFactory.getLogger(NexmarkGeneratorTest.class).info(() -> "Watermark: " + event.getWatermark());
            assertEquals(nextEvent2.getWatermark(), event.getWatermark());
        }
    }
    */

    @Test
    void testGetNextEventWatermarkOrderedStream() {
        NextEvent nextEvent = generator.next();
        for (int i = 0; i < 49; i++) {
            generator.next();
        }
        NextEvent nextEvent2 = generator.next();
        assertTrue(nextEvent.getWatermark() < nextEvent2.getWatermark());
        assertEquals(5, nextEvent2.getWatermark() - nextEvent.getWatermark());
    }

    @Test
    void testIncreaseEventsCountSoFar() {
        long eventsCountSoFar = generator.getEventsCountSoFar();
        generator.next();
        assertEquals(eventsCountSoFar + 1, generator.getEventsCountSoFar());
    }

    @Test
    void testGetFractionsConsumed() {
        double expectedFraction = 0.0;
        assertEquals(expectedFraction, generator.getFractionConsumed());
        for (int i = 0; i < 50; i++) {
            generator.next();
        }
        expectedFraction = 0.05;
        assertEquals(expectedFraction, generator.getFractionConsumed());
    }

    @Test
    void testEventsPerSecond() {
        NexmarkConfiguration nexmarkConfiguration = new NexmarkConfiguration();
        GeneratorConfig generatorConfig = new GeneratorConfig(
                nexmarkConfiguration, System.currentTimeMillis(), 1, 100000000L, 1);
        NexmarkGenerator generator = new NexmarkGenerator(generatorConfig);
        long start = System.currentTimeMillis();
        while (generator.hasNext()) {
            generator.next();
        }
        long end = System.currentTimeMillis();
        long duration = end - start;
        System.out.println("Duration: " + duration);
        System.out.println("Events per second: " + generator.getEventsCountSoFar() / (duration / 1000.0));
        assertEquals(100000000L, generator.getEventsCountSoFar());
    }
}
