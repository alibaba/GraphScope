package org.apache.giraph.combiner;

import org.apache.giraph.types.ops.NumericTypeOps;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Keeps only the message with minimum value.
 *
 */
public class LongLongMinMessageCombiner implements MessageCombiner<LongWritable, LongWritable> {
    /** Numeric type ops for the value to combine */

    public LongLongMinMessageCombiner(){

    }

    @Override
    public void combine(LongWritable vertexId, LongWritable originalMessage, LongWritable messageToCombine) {
        if (messageToCombine.get() < originalMessage.get()){
            originalMessage.set(messageToCombine.get());
        }
    }


    @Override
    public LongWritable createInitialMessage() {
        return new LongWritable(0);
    }
}
