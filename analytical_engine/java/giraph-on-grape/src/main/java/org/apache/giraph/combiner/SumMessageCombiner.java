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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Message combiner which sums all messages.
 *
 * @param <M> Message type
 */
public class SumMessageCombiner<M extends Writable>
    implements MessageCombiner<WritableComparable, M> {
    /** DoubleWritable specialization */
    public static final SumMessageCombiner<DoubleWritable> DOUBLE =
        new SumMessageCombiner<>(DoubleTypeOps.INSTANCE);
    /** DoubleWritable specialization */
    public static final SumMessageCombiner<FloatWritable> FLOAT =
        new SumMessageCombiner<>(FloatTypeOps.INSTANCE);
    /** LongWritable specialization */
    public static final SumMessageCombiner<LongWritable> LONG =
        new SumMessageCombiner<>(LongTypeOps.INSTANCE);
    /** IntWritable specialization */
    public static final SumMessageCombiner<IntWritable> INT =
        new SumMessageCombiner<>(IntTypeOps.INSTANCE);

    /** Value type operations */
    private final NumericTypeOps<M> typeOps;

    /**
     * Constructor
     * @param typeOps Value type operations
     */
    public SumMessageCombiner(NumericTypeOps<M> typeOps) {
        this.typeOps = typeOps;
    }

    @Override
    public void combine(
        WritableComparable vertexIndex, M originalMessage, M messageToCombine) {
        typeOps.plusInto(originalMessage, messageToCombine);
    }

    @Override
    public M createInitialMessage() {
        return typeOps.createZero();
    }
}
