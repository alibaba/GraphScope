package ldbc.snb.datagen.generator.generators.knowsgenerators;

import ldbc.snb.datagen.entities.dynamic.person.Person;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

public interface KnowsGenerator {
    void generateKnows(List<Person> persons, int seed, List<Float> percentages, int step_index);

    void initialize(Configuration conf);
}
