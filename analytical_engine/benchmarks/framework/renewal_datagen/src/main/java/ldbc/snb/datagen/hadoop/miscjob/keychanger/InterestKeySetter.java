package ldbc.snb.datagen.hadoop.miscjob.keychanger;

import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.hadoop.key.TupleKey;

public class InterestKeySetter implements HadoopFileKeyChanger.KeySetter<TupleKey> {

    public TupleKey getKey(Object object) {
        Person person = (Person) object;
        return new TupleKey(person.mainInterest(), person.accountId());
    }
}
