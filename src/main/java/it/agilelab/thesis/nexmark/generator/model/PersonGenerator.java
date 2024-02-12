package it.agilelab.thesis.nexmark.generator.model;

import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.model.Person;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static it.agilelab.thesis.nexmark.generator.model.StringsGenerator.nextExtra;
import static it.agilelab.thesis.nexmark.generator.model.StringsGenerator.nextString;

/**
 * Generates people.
 */
public final class PersonGenerator {
    /**
     * Number of yet-to-be-created people and auction ids allowed.
     */
    private static final int PERSON_ID_LEAD = 10;
    /**
     * Keep the number of states small so that the example queries will find results even with a small
     * batch of events.
     */
    private static final List<String> US_STATES = Arrays.asList("AZ,CA,ID,OR,WA,WY".split(","));
    private static final List<String> US_CITIES =
            Arrays.asList(
                    "Phoenix,Los Angeles,San Francisco,Boise,Portland,Bend,Redmond,Seattle,Kent,Cheyenne"
                            .split(","));
    private static final List<String> FIRST_NAMES =
            Arrays.asList("Peter,Paul,Luke,John,Saul,Vicky,Kate,Julie,Sarah,Deiter,Walter".split(","));
    private static final List<String> LAST_NAMES =
            Arrays.asList("Shultz,Abrams,Spencer,White,Bartels,Walton,Smith,Jones,Noris".split(","));

    private PersonGenerator() {
        throw new UnsupportedOperationException("This class should not be instantiated.");
    }

    /**
     * Generate and return a random person with next available id.
     *
     * @param nextEventId the next event id to be generated
     * @param random      the random generator
     * @param timestamp   the timestamp of the event
     * @param config      the generator configuration
     * @return the generated person
     */
    public static Person nextPerson(final long nextEventId,
                                    final Random random,
                                    final long timestamp,
                                    final GeneratorConfig config) {

        long id = lastBase0PersonId(config, nextEventId) + GeneratorConfig.getFirstPersonId();
        String name = nextPersonName(random);
        String email = nextEmail(random);
        String creditCard = nextCreditCard(random);
        String city = nextUSCity(random);
        String state = nextUSState(random);
        int currentSize =
                8 + name.length() + email.length() + creditCard.length() + city.length() + state.length();
        String extra = nextExtra(random, currentSize, config.getConfiguration().getAvgPersonByteSize());
        return new Person(id, name, email, creditCard, city, state, Instant.ofEpochMilli(timestamp), extra);
    }

    /**
     * Return a random person id (base 0). With base 0, we are intending the id without {@code FIRST_PERSON_ID},
     * located in {@link GeneratorConfig}.
     *
     * @param eventId the event id
     * @param random  the random generator
     * @param config  the generator configuration
     * @return the random person id
     */
    public static long nextBase0PersonId(final long eventId,
                                         final Random random,
                                         final GeneratorConfig config) {
        // Choose a random person from any of the 'active' people, plus a few 'leads'.
        // By limiting to 'active' we ensure the density of bids or auctions per person
        // does not decrease over time for long-running jobs.
        // By choosing a person id ahead of the last valid person id we will make
        // newPerson and newAuction events appear to have been swapped in time.
        long numPeople = lastBase0PersonId(config, eventId) + 1;
        long activePeople = Math.min(numPeople, config.getConfiguration().getMaxNumActivePeople());
        long n = LongGenerator.nextLong(random, activePeople + PERSON_ID_LEAD);
        return numPeople - activePeople + n;
    }

    /**
     * Return the last valid person id (ignoring FIRST_PERSON_ID). Will be the current person id if
     * due to generate a person.
     * <p>
     * In this way we ensure the correct generation of person IDs according to the trend of events over time.
     *
     * @param config  the generator configuration
     * @param eventId the event id
     * @return the last valid person id
     */
    public static long lastBase0PersonId(final GeneratorConfig config, final long eventId) {
        // current epoch based on event ID. An epoch is based also on the total proportion.
        // So in the default case, the first epoch is 0-50 eventIds, the second is 50-100, etc.
        long epoch = eventId / config.getTotalProportion();
        // offset of the current event within the current epoch
        // I.e. if the event id is 55, the epoch is the first, the offset is 5.
        long offset = eventId % config.getTotalProportion();
        if (offset >= config.getPersonProportion()) {
            // About to generate an auction or bid.
            // Go back to the last person generated in this epoch.
            offset = (long) config.getPersonProportion() - 1;
        }
        // About to generate a person.
        return epoch * config.getPersonProportion() + offset;
    }

    private static String nextUSState(final Random random) {
        return US_STATES.get(random.nextInt(US_STATES.size()));
    }

    private static String nextUSCity(final Random random) {
        return US_CITIES.get(random.nextInt(US_CITIES.size()));
    }

    private static String nextPersonName(final Random random) {
        return FIRST_NAMES.get(random.nextInt(FIRST_NAMES.size()))
                + " "
                + LAST_NAMES.get(random.nextInt(LAST_NAMES.size()));
    }

    private static String nextEmail(final Random random) {
        int maxLenFirstPart = 7;
        int maxLenSecondPart = 5;
        return nextString(random, maxLenFirstPart) + "@" + nextString(random, maxLenSecondPart) + ".com";
    }

    private static String nextCreditCard(final Random random) {
        StringBuilder sb = new StringBuilder();
        int bound = 10000;
        for (int i = 0; i < 4; i++) {
            if (i > 0) {
                sb.append(' ');
            }
            sb.append(String.format("%04d", random.nextInt(bound)));
        }
        return sb.toString();
    }

    /**
     * Returns the yet-to-be-created people and auction ids allowed.
     *
     * @return the yet-to-be-created people and auction ids allowed
     */
    public static int getPersonIdLead() {
        return PERSON_ID_LEAD;
    }
}
