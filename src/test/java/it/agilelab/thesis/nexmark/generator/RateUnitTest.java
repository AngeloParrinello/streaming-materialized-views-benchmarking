package it.agilelab.thesis.nexmark.generator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RateUnitTest {

    @Test
    void testRateToPeriodUsPerSecond() {
        testRateToPeriodUs(RateUnit.PER_SECOND, 10_000, 100);
        testRateToPeriodUs(RateUnit.PER_SECOND, 5000, 200);
    }

    @Test
    void testRateToPeriodUsPerMinute() {
        testRateToPeriodUs(RateUnit.PER_MINUTE, 1000, 60_000L);
        testRateToPeriodUs(RateUnit.PER_MINUTE, 2000, 30_000L);
    }

    // Helper method to test rateToPeriodUs for a given RateUnit
    private void testRateToPeriodUs(final RateUnit rateUnit, final long rate, final long expectedPeriodUs) {
        long actualPeriodUs = rateUnit.rateToPeriodUs(rate);
        assertEquals(expectedPeriodUs, actualPeriodUs);
    }

}
