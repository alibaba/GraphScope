package org.apache.giraph.combiner;

import org.apache.giraph.types.ops.NumericTypeOps;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Keeps only the message with minimum value.
 *
 * @param <I> Vertex id
 * @param <M> Message
 */
public class MinMessageCombiner<I extends WritableComparable,
    M extends Writable> implements MessageCombiner<I, M> {
    /** Numeric type ops for the value to combine */
    private final NumericTypeOps<M> numTypeOps;

    /**
     * Combiner
     *
     * @param numTypeOps Type ops to use
     */
    public MinMessageCombiner(NumericTypeOps<M> numTypeOps) {
        this.numTypeOps = numTypeOps;
    }

    @Override
    public void combine(I vertexId, M originalMessage, M messageToCombine) {
        if (numTypeOps.compare(originalMessage, messageToCombine) > 0) {
            numTypeOps.set(originalMessage, messageToCombine);
        }
    }

    @Override
    public M createInitialMessage() {
        return this.numTypeOps.createMaxPositiveValue();
    }
}