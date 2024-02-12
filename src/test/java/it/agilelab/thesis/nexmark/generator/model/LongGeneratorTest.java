package it.agilelab.thesis.nexmark.generator.model;

import org.junit.jupiter.api.Test;
import org.junit.platform.commons.logging.LoggerFactory;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LongGeneratorTest {
    @Test
    void testNextLong() {
        Random random = new Random();
        long n = 10000000L;
        long generatedLong = LongGenerator.nextLong(random, n);
        LoggerFactory.getLogger(LongGeneratorTest.class).info(() -> "The generated long is = " + generatedLong);
        assertTrue(n >= generatedLong);
    }
}
