package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.hadoop.key.TupleKey;
import org.apache.hadoop.mapreduce.Partitioner;

public class HadoopTuplePartitioner extends Partitioner<TupleKey, Person> {

    @Override
    public int getPartition(TupleKey key, Person person, int numReduceTasks) {
        return (int) (key.key % numReduceTasks);
    }
}
