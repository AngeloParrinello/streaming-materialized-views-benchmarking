package it.agilelab.thesis.nexmark.generator;

/**
 * Units for rates.
 */
public enum RateUnit {
    /**
     * Number of microseconds per second.
     */
    PER_SECOND(1_000_000L),
    /**
     * Number of microseconds per minute.
     */
    PER_MINUTE(60_000_000L);

    /**
     * Number of microseconds per unit.
     */
    private final long usPerUnit;

    RateUnit(final long usPerUnit) {
        this.usPerUnit = usPerUnit;
    }

    /**
     * Number of microseconds between events at given rate.
     * This method calculates the number of microseconds between two events given one event rate per second (rate).
     * <p>
     * Here is how the calculation works:
     * <p>
     * 1. The variable usPerUnit represents the number of microseconds per unit of time,
     * which is initialized in the enum constructor. In our case, we defined two units of time: PER_SECOND and
     * PER_MINUTE, with the corresponding microsecond values per unit.
     * <p>
     * 2. The formula (this.usPerUnit + rate / 2) / rate calculates the number of microseconds
     * between two events at the specified rate. Firstly, (this.usPerUnit + rate / 2) adds half the rate
     * to the amount of microseconds per unit. This is done to get a correct rounding. For example,
     * if the rate is 5 and usPerUnit is 1_000_000, adding 5/2(=2) (5/2 * 1_000_000) we will have 1_000_002 microseconds.
     * Secondly, ((this.usPerUnit + rate / 2) / rate) divides the result obtained in the previous
     * step by the specified rate. This calculates the number of microseconds between two events at a given rate.
     * In the example above, dividing 1_000_002 by 5 gives 200_000.4 (=200_000) microseconds.
     * <p>
     * In this way, the rateToPeriodUs function returns the number of microseconds between two events at
     * the specified rate, considering the correct handling of rounding
     *
     * @param rate Rate in events.
     * @return Number of microseconds between events at given rate.
     */
    public long rateToPeriodUs(final long rate) {
        return (this.usPerUnit + (rate / 2)) / rate;
    }

    /**
     * Get the amount of microsecond contained in the unit of time selected.
     *
     * @return the amount of microsecond contained in the unit of time selected.
     */
    public long getUsPerUnit() {
        return this.usPerUnit;
    }

    /**
     * Generated toString method, used to print object's internal value.
     *
     * @return the string related to the object.
     */
    @Override
    public String toString() {
        return "RateUnit{"
                + "usPerUnit=" + usPerUnit
                + '}';
    }
}
