
package ldbc.snb.datagen.util;

import java.util.Random;

public class RandomGeneratorFarm {

    private int numRandomGenerators;
    private Random[] randomGenerators;

    public enum Aspect {
        DATE,
        BIRTH_DAY,
        KNOWS_REQUEST,
        INITIATOR,
        UNIFORM,
        NUM_INTEREST,
        NUM_TAG,
        NUM_KNOWS,
        NUM_COMMENT,
        NUM_PHOTO_ALBUM,
        NUM_PHOTO,
        NUM_FORUM,
        NUM_USERS_PER_FORUM,
        NUM_POPULAR,
        NUM_LIKE,
        NUM_POST,
        KNOWS,
        KNOWS_LEVEL,
        GENDER,
        RANDOM,
        MEMBERSHIP,
        MEMBERSHIP_INDEX,
        FORUM,
        FORUM_MODERATOR,
        FORUM_INTEREST,
        EXTRA_INFO,
        EXACT_LONG_LAT,
        STATUS,
        HAVE_STATUS,
        STATUS_SINGLE,
        USER_AGENT,
        USER_AGENT_SENT,
        FILE_SELECT,
        IP,
        DIFF_IP_FOR_TRAVELER,
        DIFF_IP,
        BROWSER,
        DIFF_BROWSER,
        CITY,
        COUNTRY,
        TAG,
        UNIVERSITY,
        UNCORRELATED_UNIVERSITY,
        UNCORRELATED_UNIVERSITY_LOCATION,
        TOP_UNIVERSITY,
        POPULAR,
        EMAIL,
        TOP_EMAIL,
        COMPANY,
        UNCORRELATED_COMPANY,
        UNCORRELATED_COMPANY_LOCATION,
        LANGUAGE,
        ALBUM,
        ALBUM_MEMBERSHIP,
        NAME,
        SURNAME,
        TAG_OTHER_COUNTRY,
        SET_OF_TAG,
        TEXT_SIZE,
        REDUCED_TEXT,
        LARGE_TEXT,
        MEMBERSHIP_POST_CREATOR,
        REPLY_TO,
        TOPIC,
        NUM_ASPECT                  // This must be always the last one.
    }

    public RandomGeneratorFarm() {
        numRandomGenerators = Aspect.values().length;
        randomGenerators = new Random[numRandomGenerators];
        for (int i = 0; i < numRandomGenerators; ++i) {
            randomGenerators[i] = new Random();
        }
    }

    public Random get(Aspect aspect) {
        return randomGenerators[aspect.ordinal()];
    }

    public void resetRandomGenerators(long seed) {
        Random seedRandom = new Random(53223436L + 1234567 * seed);
        for (int i = 0; i < numRandomGenerators; i++) {
            randomGenerators[i].setSeed(seedRandom.nextLong());
        }
    }
}
