package it.agilelab.thesis.nexmark.model;

import java.util.Objects;

/**
 * A simple pair of values.
 *
 * @param <X> the type of the first value
 * @param <Y> the type of the second value
 */
public class Pair<X, Y> {
    private final X first;
    private final Y second;

    public Pair(final X first, final Y second) {
        this.first = first;
        this.second = second;
    }

    /**
     * Get the first value.
     *
     * @return the first value
     */
    public X getFirst() {
        return this.first;
    }

    /**
     * Get the second value.
     *
     * @return the second value
     */
    public Y getSecond() {
        return this.second;
    }

    /**
     * Generated equals method for comparing two objects.
     *
     * @param o the object to compare with.
     * @return true if those two objects are the same.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Pair)) {
            return false;
        }
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(getFirst(), pair.getFirst()) && Objects.equals(getSecond(), pair.getSecond());
    }

    /**
     * Generated hashCode method, used to generate the hash values of objects.
     *
     * @return an integer whose value represents the hash value of the input object
     */
    @Override
    public int hashCode() {
        return Objects.hash(getFirst(), getSecond());
    }

    /**
     * Generated toString method, used to print object's internal value.
     *
     * @return the string related to the object.
     */
    @Override
    public String toString() {
        return "<" + this.first + "," + this.second + ">";
    }
}
