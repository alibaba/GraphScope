package ldbc.snb.datagen.entities.dynamic.person.similarity;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;

public class GeoDistanceSimilarity implements Person.PersonSimilarity {
    @Override
    public float similarity(Person personA, Person personB) {
        int zorderA = Dictionaries.places.getZorderID(personA.countryId());
        int zorderB = Dictionaries.places.getZorderID(personB.countryId());
        return 1.0f - (Math.abs(zorderA - zorderB) / 256.0f);
    }
}
