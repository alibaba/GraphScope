/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.graph;

import org.apache.hadoop.io.Writable;

/**
 * Define the interface for a global communicator. Implementations should take care of all fragment
 * inside aggregation and between-fragment aggregation.
 */
public interface Communicator {

    /**
     * Add a new value
     *
     * @param name  Name of aggregator
     * @param value Value to add
     * @param <A>   Aggregated value
     */
    <A extends Writable> void aggregate(String name, A value);

    /**
     * Get value of an aggregator.
     *
     * @param name Name of aggregator
     * @param <A>  Aggregated value
     * @return Value of the aggregator
     */
    <A extends Writable> A getAggregatedValue(String name);

    /**
     * Get value broadcasted from master
     *
     * @param name Name of the broadcasted value
     * @param <B>  Broadcast value type
     * @return Broadcasted value
     */
    <B extends Writable> B getBroadcast(String name);

    /**
     * Reduce given value.
     *
     * @param name  Name of the reducer
     * @param value Single value to reduce
     */
    void reduce(String name, Object value);

    /**
     * Reduce given partial value.
     *
     * @param name  Name of the reducer
     * @param value Single value to reduce
     */
    void reduceMerge(String name, Writable value);
}
