package it.agilelab.thesis.nexmark.generator;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RateShapeTest {

    private static RateShape rateShape;
    private static RateUnit unit;
    private static int numGenerators;

    @BeforeAll
    static void setup() {
        rateShape = RateShape.SQUARE;
        unit = RateUnit.PER_SECOND;
        numGenerators = 3;
    }

    @Test
    void testInterEventDelayUsSingleRate() {
        int rate = 100; // 100 events per second
        long singleDelay = 10_000; // 10_000 microseconds
        long totalExpectedDelay = singleDelay * numGenerators;
        long actualDelay = rateShape.interEventDelayUs(rate, unit, numGenerators);
        assertEquals(totalExpectedDelay, actualDelay);
    }

    @Test
    void testInterEventDelayUsMultipleRateLength() {
        int initialRate = 10_000;
        int nextRate = 10_000;
        int expectedLength = 1;
        assertEquals(expectedLength, rateShape.interEventDelayUs(initialRate, nextRate, unit, numGenerators).length);
    }

    @Test
    void testInterEventDelayUsMultipleSameRate() {
        int initialRate = 10_000;
        int nextRate = 10_000;
        long singleDelay = 100;
        long totalExpectedDelay = singleDelay * numGenerators;
        assertEquals(totalExpectedDelay, rateShape.interEventDelayUs(initialRate, nextRate, unit, numGenerators)[0]);
    }

    @Test
    void testInterEventDelayUsMultipleDifferentRate() {
        int initialRate = 10_000;
        int nextRate = 20_000;
        long singleInitialDelay = 100;
        long singleNextDelay = 50;
        long totalInitialExpectedDelay = singleInitialDelay * numGenerators;
        long totalNextExpectedDelay = singleNextDelay * numGenerators;
        assertEquals(totalInitialExpectedDelay, rateShape.interEventDelayUs(initialRate, nextRate, unit, numGenerators)[0]);
        assertEquals(totalNextExpectedDelay, rateShape.interEventDelayUs(initialRate, nextRate, unit, numGenerators)[1]);
    }

    @Test
    void testInterEventDelayUsMultipleDifferentRateSineShape() {
        int initialRate = 100;
        int nextRate = 200;
        long[] totalExpectedDelays = {100_000, 90_910, 74_070, 60_610, 52_630, 50_000, 52_630, 60_610, 74_070, 90_910};
        long[] totalActualDelays = RateShape.SINE.interEventDelayUs(initialRate, nextRate, unit, 10);

        assertEquals(totalExpectedDelays.length, totalActualDelays.length);
        IntStream.range(0, totalExpectedDelays.length).forEach(i -> assertEquals(totalExpectedDelays[i], totalActualDelays[i]));
    }

    @Test
    void testStepLength() {
        int ratePeriodSec = 600;
        int expectedSteps = 300;
        assertEquals(expectedSteps, rateShape.stepLengthSec(ratePeriodSec));
        int expectedSineSteps = 60;
        assertEquals(expectedSineSteps, RateShape.SINE.stepLengthSec(ratePeriodSec));
    }

}
