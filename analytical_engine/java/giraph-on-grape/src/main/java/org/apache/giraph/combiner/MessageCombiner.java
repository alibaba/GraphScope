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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Abstract class to extend for combining messages sent to the same vertex. MessageCombiner for
 * applications where each two messages for one vertex can be combined into one.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public interface MessageCombiner<I extends WritableComparable, M extends Writable> {

    /**
     * Combine messageToCombine with originalMessage, by modifying originalMessage.  Note that the
     * messageToCombine object may be reused by the infrastructure, therefore, you cannot directly
     * use it or any objects from it in original message
     *
     * @param vertexIndex      Index of the vertex getting these messages
     * @param originalMessage  The first message which we want to combine; put the result of
     *                         combining in this message
     * @param messageToCombine The second message which we want to combine (object may be reused -
     *                         do not reference it or its member objects)
     */
    void combine(I vertexIndex, M originalMessage, M messageToCombine);

    /**
     * Get the initial message. When combined with any other message M, the result should be M.
     *
     * @return Initial message
     */
    M createInitialMessage();
}
