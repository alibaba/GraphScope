package org.apache.giraph.combiner;

import org.apache.giraph.types.ops.DoubleTypeOps;
import org.apache.giraph.types.ops.FloatTypeOps;
import org.apache.giraph.types.ops.IntTypeOps;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.giraph.types.ops.NumericTypeOps;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Message combiner which calculates max of all messages.
 *
 * @param <M> Message type
 */
public class MaxMessageCombiner<M extends WritableComparable>
    implements MessageCombiner<WritableComparable, M> {
    /** DoubleWritable specialization */
    public static final MaxMessageCombiner<DoubleWritable> DOUBLE =
        new MaxMessageCombiner<>(DoubleTypeOps.INSTANCE);
    /** DoubleWritable specialization */
    public static final MaxMessageCombiner<FloatWritable> FLOAT =
        new MaxMessageCombiner<>(FloatTypeOps.INSTANCE);
    /** LongWritable specialization */
    public static final MaxMessageCombiner<LongWritable> LONG =
        new MaxMessageCombiner<>(LongTypeOps.INSTANCE);
    /** IntWritable specialization */
    public static final MaxMessageCombiner<IntWritable> INT =
        new MaxMessageCombiner<>(IntTypeOps.INSTANCE);

    /** Value type operations */
    private final NumericTypeOps<M> typeOps;

    /**
     * Constructor
     * @param typeOps Value type operations
     */
    public MaxMessageCombiner(NumericTypeOps<M> typeOps) {
        this.typeOps = typeOps;
    }

    @Override
    public void combine(
        WritableComparable vertexIndex, M originalMessage, M messageToCombine) {
        if (originalMessage.compareTo(messageToCombine) < 0) {
            typeOps.set(originalMessage, messageToCombine);
        }
    }

    @Override
    public M createInitialMessage() {
        return typeOps.createZero();
    }
}
