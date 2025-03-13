package ldbc.snb.datagen.generator.generators.knowsgenerators;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

public class DistanceKnowsGenerator implements KnowsGenerator {

    private RandomGeneratorFarm randomFarm;

    public DistanceKnowsGenerator() {
        this.randomFarm = new RandomGeneratorFarm();
    }

    public void generateKnows(List<Person> persons, int seed, List<Float> percentages, int step_index) {
        randomFarm.resetRandomGenerators(seed);
        for (int i = 0; i < persons.size(); ++i) {
            Person p = persons.get(i);
            for (int j = i + 1; (Knows.targetEdges(p, percentages, step_index) > p.knows().size()) && (j < persons
                    .size()); ++j) {
                if (know(p, persons.get(j), j - i, percentages, step_index)) {
                    Knows.createKnow(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), p, persons.get(j));
                }
            }
        }
    }

    @Override
    public void initialize(Configuration conf) {
        // This is inherited from knows generator and no initialization is required.
    }

    private boolean know(Person personA, Person personB, int dist, List<Float> percentages, int step_index) {
        if (personA.knows().size() >= Knows.targetEdges(personA, percentages, step_index) ||
                personB.knows().size() >= Knows.targetEdges(personB, percentages, step_index)) return false;
        double randProb = randomFarm.get(RandomGeneratorFarm.Aspect.UNIFORM).nextDouble();
        double prob = Math.pow(DatagenParams.baseProbCorrelated, dist);
        return ((randProb < prob) || (randProb < DatagenParams.limitProCorrelated));
    }

}
