package it.agilelab.thesis.nexmark.generator.model;

import java.util.Random;

public final class LongGenerator {
    private LongGenerator() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Return a random long from {@code [0, n)}.
     *
     * @param random random number generator
     * @param n      upper bound (exclusive)
     * @return a random long from {@code [0, n)}
     */
    public static long nextLong(final Random random, final long n) {
        if (n < Integer.MAX_VALUE) {
            return random.nextInt((int) n);
        } else {
            // LoggerFactory.getLogger(LongGenerator.class).info("The generated long is = {}", generatedLong);
            // LoggerFactory.getLogger(LongGenerator.class).info("WARNING: Very skewed distribution! Bad!");
            return Math.abs(random.nextLong() % n);
        }
    }
}
