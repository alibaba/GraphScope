package ldbc.snb.datagen.generator.tools;

import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;

import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PersonGraph {
    private Map<Long, HashSet<Long>> adjacencies_;

    public PersonGraph(List<Person> persons) {
        adjacencies_ = new HashMap<>();
        for (Person p : persons) {
            HashSet<Long> neighbors = new HashSet<>();
            for (Knows k : p.knows()) {
                neighbors.add(k.to().accountId());
            }
            adjacencies_.put(p.accountId(), neighbors);
        }
    }

    public Set<Long> persons() {
        return adjacencies_.keySet();
    }

    public Set<Long> neighbors(Long person) {
        return adjacencies_.get(person);
    }
}
