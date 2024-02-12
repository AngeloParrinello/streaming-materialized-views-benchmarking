package it.agilelab.thesis.nexmark.generator.model;

import java.util.Random;

/**
 * Generates strings which are used for different field in other model objects.
 */
@SuppressWarnings("checkstyle:MagicNumber")
public final class StringsGenerator {
    /**
     * Smallest random string size.
     */
    private static final int MIN_STRING_LENGTH = 3;

    private StringsGenerator() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Return a random string of up to {@code maxLength}. The string will be trimmed to remove trailing spaces.
     *
     * @param random    random number generator
     * @param maxLength maximum length of the string
     * @return a random string of up to {@code maxLength}
     */
    public static String nextString(final Random random, final int maxLength) {
        return nextString(random, maxLength, ' ');
    }

    /**
     * Return a random string of up to {@code maxLength} with {@code special} character.
     * The string will be trimmed to remove trailing spaces.
     * <p>
     * During generation, there is a small probability that a special character (special) will be included
     * otherwise random characters between 'a' and 'z' will be included. The resulting string is then
     * returned after having been subjected to a removal of the possible initial or final blank spaces.
     *
     * @param random    random number generator
     * @param maxLength maximum length of the string
     * @param special   special character
     * @return a random string of up to {@code maxLength}
     */
    public static String nextString(final Random random, final int maxLength, final char special) {
        int len = MIN_STRING_LENGTH + random.nextInt(maxLength - MIN_STRING_LENGTH);
        StringBuilder sb = new StringBuilder();
        while (len-- > 0) {
            if (random.nextInt(13) == 0) {
                sb.append(special);
            } else {
                sb.append((char) ('a' + random.nextInt(26)));
            }
        }
        return sb.toString().trim();
    }

    /**
     * Return a random string of exactly {@code length}. The string will be trimmed to remove trailing spaces.
     *
     * @param random random number generator
     * @param length length of the string
     * @return a random string of exactly {@code length}
     */
    public static String nextExactString(final Random random, final int length) {
        int myLength = length;
        StringBuilder sb = new StringBuilder();
        int rnd = 0;
        int n = 0; // number of random characters left in rnd
        while (myLength-- > 0) {
            if (n == 0) {
                rnd = random.nextInt();
                n = 6; // log_26(2^31)
            }
            sb.append((char) ('a' + rnd % 26));
            rnd /= 26;
            n--;
        }
        return sb.toString();
    }

    /**
     * Return a random {@code string} such that {@code currentSize + string.length()} is on average
     * {@code averageSize}.
     *
     * @param random             random number generator
     * @param currentSize        current size of the string
     * @param desiredAverageSize desired average size of the string
     * @return a random {@code string} such that {@code currentSize + string.length()} is on average
     */
    public static String nextExtra(final Random random, final int currentSize, final int desiredAverageSize) {
        int myDesiredAverageSize = desiredAverageSize;
        if (currentSize > myDesiredAverageSize) {
            return "";
        }
        myDesiredAverageSize -= currentSize;
        int delta = (int) Math.round(myDesiredAverageSize * 0.2);
        int minSize = myDesiredAverageSize - delta;
        int desiredSize = minSize + (delta == 0 ? 0 : random.nextInt(2 * delta));
        return nextExactString(random, desiredSize);
    }
}
