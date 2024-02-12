package it.agilelab.thesis.nexmark.generator.model;

import java.util.Random;

/**
 * Generates a random price.
 */
public final class PriceGenerator {
    private PriceGenerator() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Return a random price.
     *
     * @param random the random number generator
     * @return a random price
     */
    public static long nextPrice(final Random random) {
        return Math.round(Math.pow(10.0, random.nextDouble() * 6.0) * 100.0);
    }
}
