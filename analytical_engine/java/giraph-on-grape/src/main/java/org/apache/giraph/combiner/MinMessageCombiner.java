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

import org.apache.giraph.types.ops.NumericTypeOps;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Keeps only the message with minimum value.
 *
 * @param <I> Vertex id
 * @param <M> Message
 */
public class MinMessageCombiner<I extends WritableComparable, M extends Writable>
        implements MessageCombiner<I, M> {

    /**
     * Numeric type ops for the value to combine
     */
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
