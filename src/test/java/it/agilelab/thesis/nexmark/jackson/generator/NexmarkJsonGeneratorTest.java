package it.agilelab.thesis.nexmark.jackson.generator;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

class NexmarkJsonGeneratorTest {

    private NexmarkJsonGenerator generator;

    @BeforeEach
    void setup() {
        long firstEventId = 1;
        long maxEvents = 1000;
        long firstEventNumber = 0;
        long baseTime = System.currentTimeMillis();

        NexmarkConfiguration nexmarkConfiguration = new NexmarkConfiguration();

        GeneratorConfig config = new GeneratorConfig(nexmarkConfiguration,
                baseTime, firstEventId, maxEvents, firstEventNumber);

        generator = new NexmarkJsonGenerator(config);
    }

    @Test
    void testDemoGenerate() throws JsonProcessingException {
        int count = 0;
        while (generator.hasNext()) {
            String event = generator.nextJsonEvent();
            count++;
            System.out.println(event);
        }
        assertEquals(1000, count);
    }

    @Test
    void testEventsPerSecond() {
        NexmarkConfiguration nexmarkConfiguration = new NexmarkConfiguration();
        GeneratorConfig generatorConfig = new GeneratorConfig(
                nexmarkConfiguration, System.currentTimeMillis(), 1, 100000000L, 1);
        NexmarkJsonGenerator generator = new NexmarkJsonGenerator(generatorConfig);
        long start = System.currentTimeMillis();
        long end = start + 1000;
        int events = 0;
        while (System.currentTimeMillis() < end) {
            generator.next();
            events++;
        }
        System.out.println(events);
        assertTrue(events > 100_000);
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
        while (generator.hasNext()) {
            generator.next();
        }
        assertThrows(NoSuchElementException.class, generator::next);
    }

}
