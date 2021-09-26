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
package org.apache.giraph.conf;

import com.alibaba.graphscope.parallel.message.MessageEncodeAndStoreType;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Interface for containing all items that define message being sent, including it's value factory
 * and combiner.
 *
 * @param <I>
 * @param <M>
 */
public interface MessageClasses<I extends WritableComparable, M extends Writable> extends Writable {

    /**
     * Get message class
     *
     * @return message class
     */
    Class<M> getMessageClass();

    /**
     * Create new instance of MessageValueFactory
     *
     * @param conf Configuration
     * @return message value factory
     */
    MessageValueFactory<M> createMessageValueFactory(ImmutableClassesGiraphConfiguration conf);

    /**
     * Create new instance of MessageCombiner
     *
     * @param conf Configuration
     * @return message combiner
     */
    MessageCombiner<? super I, M> createMessageCombiner(
            ImmutableClassesGiraphConfiguration<I, ? extends Writable, ? extends Writable> conf);

    /**
     * Has message combiner been specified
     *
     * @return has message combiner been specified
     */
    boolean useMessageCombiner();

    /**
     * Get MessageEncodeAndStoreType
     *
     * @return message encode and store type
     */
    MessageEncodeAndStoreType getMessageEncodeAndStoreType();

    /**
     * Creates a fresh copy of this object, to be used and changed for new superstep. (that should
     * be independent from the previous one)
     *
     * @return message classes
     */
    MessageClasses<I, M> createCopyForNewSuperstep();

    /**
     * Verify if types are internally consistent
     *
     * @param conf Configuration
     */
    void verifyConsistent(ImmutableClassesGiraphConfiguration conf);

    /**
     * Whether to completely ignore existing vertices, and just process messages
     *
     * @return ignoreExistingVertices
     */
    boolean ignoreExistingVertices();
}
