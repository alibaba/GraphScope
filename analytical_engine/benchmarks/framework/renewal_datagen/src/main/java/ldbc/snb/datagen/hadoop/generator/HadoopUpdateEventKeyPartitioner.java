package ldbc.snb.datagen.hadoop.generator;

import ldbc.snb.datagen.hadoop.key.updatekey.UpdateEventKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class HadoopUpdateEventKeyPartitioner extends Partitioner<UpdateEventKey, Text> {

    @Override
    public int getPartition(UpdateEventKey key, Text text, int numReduceTasks) {
        return (key.reducerId);
    }
}

