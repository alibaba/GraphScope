package ldbc.snb.datagen.entities.dynamic.person.similarity;

import ldbc.snb.datagen.entities.dynamic.person.Person;

import java.util.Set;
import java.util.TreeSet;

public class InterestsSimilarity implements Person.PersonSimilarity {
    public float similarity(Person personA, Person personB) {
        Set<Integer> union = new TreeSet<>(personA.interests());
        union.addAll(personB.interests());
        union.add(personA.mainInterest());
        union.add(personB.mainInterest());
        Set<Integer> intersection = new TreeSet<>(personA.interests());
        intersection.retainAll(personB.interests());
        if (personA.mainInterest() == personB.mainInterest()) intersection.add(personA.mainInterest());
        return union.size() > 0 ? intersection.size() / (float) union.size() : 0;
    }
}
