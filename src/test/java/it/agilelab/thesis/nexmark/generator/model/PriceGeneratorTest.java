package it.agilelab.thesis.nexmark.generator.model;

import org.junit.jupiter.api.Test;
import org.junit.platform.commons.logging.LoggerFactory;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

class PriceGeneratorTest {
    @Test
    void testNextPrice() {
        long price = PriceGenerator.nextPrice(new Random());
        LoggerFactory.getLogger(PriceGeneratorTest.class).info(() -> "The generated price is: " + price);
        assertNotEquals(0, PriceGenerator.nextPrice(new Random()));
    }
}
