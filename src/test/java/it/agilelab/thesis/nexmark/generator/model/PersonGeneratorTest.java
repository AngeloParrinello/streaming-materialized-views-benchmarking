package it.agilelab.thesis.nexmark.generator.model;

import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.model.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.logging.LoggerFactory;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PersonGeneratorTest {
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
    void testFirstNextPerson() {
        long adjustedEventTimestamp = config.timestampForEvent(config.nextAdjustedEventNumber(0));
        Person person = PersonGenerator.nextPerson(config.getFirstEventId(), random, adjustedEventTimestamp, config);
        LoggerFactory.getLogger(PersonGeneratorTest.class).info(() -> "The generated person is: " + person);
        // TODO: this is really awful ... we should call the firstPersonId method from inside the config
        assertEquals(GeneratorConfig.getFirstPersonId(), person.getId());
    }

    @Test
    void testLastBase0PersonId() {
        int eventId = 345;
        long epoch = eventId / config.getTotalProportion();
        long offset = eventId % config.getTotalProportion();
        System.out.println("The epoch is: " + epoch);
        System.out.println("The offset is: " + offset);
        long total;
        if (offset >= config.getPersonProportion()) {
            offset = config.getPersonProportion() - 1;

        }
        total = epoch * config.getPersonProportion() + offset;
        System.out.println("The total is: " + total);
        System.out.println("The last base 0 person id is: " + PersonGenerator.lastBase0PersonId(config, eventId));
        assertEquals(total, PersonGenerator.lastBase0PersonId(config, eventId));
    }

    @Test
    void testNextBase0PersonId() {
        int eventId = 531;
        // Not relevant for this test, but I needed to see it to understand the test
        long numPeople = PersonGenerator.lastBase0PersonId(config, eventId) + 1;
        long activePeople = Math.min(numPeople, config.getConfiguration().getMaxNumActivePeople());
        long n = LongGenerator.nextLong(random, activePeople + PersonGenerator.getPersonIdLead());
        long total = numPeople - activePeople + n;
        System.out.println("The num people is: " + numPeople);
        System.out.println("The active people is: " + activePeople);
        System.out.println("The number is: " + n);
        System.out.println("The total is: " + total);
        // This part is the only relevant one for the test
        // We cannot do this kind of test because there is a random component
        // assertEquals(total, PersonGenerator.nextBase0PersonId(eventId, random, config));
        long total2 = PersonGenerator.nextBase0PersonId(eventId, random, config);
        System.out.println("The total2 is: " + total2);
        assertTrue(total2 <= 21);
    }

    @Test
    void testNextPerson() {
        int eventId = 321;
        long adjustedEventTimestamp = config.timestampForEvent(config.nextAdjustedEventNumber(eventId));
        Person person = PersonGenerator.nextPerson(eventId, random, adjustedEventTimestamp, config);
        System.out.println("The generated person is: " + person);
        // 321 / 50 = 6 => base 0 (1000) + 6 = 1006
        assertEquals(1006, person.getId());
    }
}
