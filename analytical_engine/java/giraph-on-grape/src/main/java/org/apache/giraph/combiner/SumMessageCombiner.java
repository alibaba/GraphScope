/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    /**
     * DoubleWritable specialization
     */
    public static final SumMessageCombiner<DoubleWritable> DOUBLE =
            new SumMessageCombiner<>(DoubleTypeOps.INSTANCE);
    /**
     * DoubleWritable specialization
     */
    public static final SumMessageCombiner<FloatWritable> FLOAT =
            new SumMessageCombiner<>(FloatTypeOps.INSTANCE);
    /**
     * LongWritable specialization
     */
    public static final SumMessageCombiner<LongWritable> LONG =
            new SumMessageCombiner<>(LongTypeOps.INSTANCE);
    /**
     * IntWritable specialization
     */
    public static final SumMessageCombiner<IntWritable> INT =
            new SumMessageCombiner<>(IntTypeOps.INSTANCE);

    /**
     * Value type operations
     */
    private final NumericTypeOps<M> typeOps;

    /**
     * Constructor
     *
     * @param typeOps Value type operations
     */
    public SumMessageCombiner(NumericTypeOps<M> typeOps) {
        this.typeOps = typeOps;
    }

    @Override
    public void combine(WritableComparable vertexIndex, M originalMessage, M messageToCombine) {
        typeOps.plusInto(originalMessage, messageToCombine);
    }

    @Override
    public M createInitialMessage() {
        return typeOps.createZero();
    }
}
