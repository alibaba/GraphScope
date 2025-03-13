package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.hadoop.key.blockkey.BlockKey;
import org.apache.hadoop.mapreduce.Partitioner;

public class HadoopBlockPartitioner extends Partitioner<BlockKey, Person> {

    @Override
    public int getPartition(BlockKey key, Person person, int numReduceTasks) {
        return (int) (key.block % numReduceTasks);
    }
}
