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

package org.apache.giraph.aggregators;

import org.apache.hadoop.io.Writable;

/**
 * Interface for Aggregator.  Allows aggregate operations for all vertices
 * in a given superstep.
 *
 * @param <A> Aggregated value
 */
public interface Aggregator<A extends Writable> {
    /**
     * Add a new value.
     * Needs to be commutative and associative
     *
     * @param value Value to be aggregated.
     */
    void aggregate(A value);

    /**
     * Return new aggregated value which is neutral to aggregate operation.
     * Must be changeable without affecting internals of Aggregator
     *
     * @return Neutral value
     */
    A createInitialValue();

    /**
     * Return current aggregated value.
     * Needs to be initialized if aggregate or setAggregatedValue
     * have not been called before.
     *
     * @return Aggregated
     */
    A getAggregatedValue();

    /**
     * Set aggregated value.
     * Can be used for initialization or reset.
     *
     * @param value Value to be set.
     */
    void setAggregatedValue(A value);

    /**
     * Reset the value of aggregator to neutral value
     */
    void reset();
}
