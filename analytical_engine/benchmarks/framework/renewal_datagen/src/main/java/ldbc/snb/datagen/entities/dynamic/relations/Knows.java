package ldbc.snb.datagen.entities.dynamic.relations;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Random;


public class Knows implements Writable, Comparable<Knows> {

    private long creationDate_;
    private Person.PersonSummary to_ = null;
    private float weight_ = 0.0f;
    public static int num = 0;

    public Knows() {
        to_ = new Person.PersonSummary();
    }

    public Knows(Knows k) {
        to_ = new Person.PersonSummary(k.to());
        creationDate_ = k.creationDate();
        weight_ = k.weight();
    }

    public Knows(Person to, long creationDate, float weight) {
        to_ = new Person.PersonSummary(to);
        creationDate_ = creationDate;
        weight_ = weight;
    }

    public Person.PersonSummary to() {
        return to_;
    }

    public void to(Person.PersonSummary to) {
        to_.copy(to);
    }

    public long creationDate() {
        return creationDate_;
    }

    public void creationDate(long creationDate) {
        creationDate_ = creationDate;
    }

    public void weight(float weight) {
        weight_ = weight;
    }

    public float weight() {
        return weight_;
    }

    public void readFields(DataInput arg0) throws IOException {
        to_.readFields(arg0);
        creationDate_ = arg0.readLong();
        weight_ = arg0.readFloat();
    }

    public void write(DataOutput arg0) throws IOException {
        to_.write(arg0);
        arg0.writeLong(creationDate_);
        arg0.writeFloat(weight_);
    }

    public int compareTo(Knows k) {
        long res = (to_.accountId() - k.to().accountId());
        if (res > 0) return 1;
        if (res < 0) return -1;
        return 0;
    }

    static public class FullComparator implements Comparator<Knows> {

        public int compare(Knows a, Knows b) {
            long res = (a.to_.accountId() - b.to().accountId());
            if (res > 0) return 1;
            if (res < 0) return -1;
            long res2 = a.creationDate_ - b.creationDate();
            if (res2 > 0) return 1;
            if (res2 < 0) return -1;
            return 0;
        }

    }

    public static boolean createKnow(Random random, Person personA, Person personB) {
        long creationDate = Dictionaries.dates.randomKnowsCreationDate(
                random,
                personA,
                personB);
        creationDate = creationDate - personA
                .creationDate() >= DatagenParams.deltaTime ? creationDate : creationDate + (DatagenParams.deltaTime - (creationDate - personA
                .creationDate()));
        creationDate = creationDate - personB
                .creationDate() >= DatagenParams.deltaTime ? creationDate : creationDate + (DatagenParams.deltaTime - (creationDate - personB
                .creationDate()));
        float similarity = Person.personSimilarity.similarity(personA, personB);
        return personB.knows().add(new Knows(personA, creationDate, similarity)) &&
                personA.knows().add(new Knows(personB, creationDate, similarity));
    }

    public static long targetEdges(Person person, List<Float> percentages, int step_index) {
        int generated_edges = 0;
        for (int i = 0; i < step_index; ++i) {
            generated_edges += Math.ceil(percentages.get(i) * person.maxNumKnows());
        }
        generated_edges = Math.min(generated_edges, (int) person.maxNumKnows());
        int to_generate = Math.min((int) person.maxNumKnows() - generated_edges, (int) Math
                .ceil(percentages.get(step_index) * person.maxNumKnows()));
        return to_generate;
    }
}
