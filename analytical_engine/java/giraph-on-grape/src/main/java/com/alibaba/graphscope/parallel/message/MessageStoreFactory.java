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

package com.alibaba.graphscope.parallel.message;

import com.alibaba.graphscope.fragment.IFragment;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Factory for message stores
 *
 * @param <I> Vertex id
 * @param <M> Message data
 * @param <MS> Message store
 */
public interface MessageStoreFactory<I extends WritableComparable, M extends Writable, MS> {
    /**
     * Creates new message store.
     *
     * @param messageClasses Message classes information to be held in the store
     * @return New message store
     */
    MS newStore(MessageClasses<I, M> messageClasses);

    /**
     * Implementation class should use this method of initialization of any required internal state.
     *
     * @param fragment fragment used for partition querying
     * @param conf Configuration
     */
    void initialize(IFragment fragment, ImmutableClassesGiraphConfiguration<I, ?, ?> conf);
}
