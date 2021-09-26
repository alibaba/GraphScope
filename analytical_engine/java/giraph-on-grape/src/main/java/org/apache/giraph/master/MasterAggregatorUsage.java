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

package org.apache.giraph.master;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.aggregators.AggregatorUsage;
import org.apache.hadoop.io.Writable;

/**
 * Master compute can access and change aggregators through this interface
 */
public interface MasterAggregatorUsage extends AggregatorUsage {
    /**
     * Register an aggregator in preSuperstep() and/or preApplication(). This
     * aggregator will have its value reset at the end of each super step.
     *
     * @param name of aggregator
     * @param aggregatorClass Class type of the aggregator
     * @param <A> Aggregator type
     * @return True iff aggregator wasn't already registered
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    <A extends Writable> boolean registerAggregator(String name,
        Class<? extends Aggregator<A>> aggregatorClass) throws
        InstantiationException, IllegalAccessException;

    /**
     * Register persistent aggregator in preSuperstep() and/or
     * preApplication(). This aggregator will not reset value at the end of
     * super step.
     *
     * @param name of aggregator
     * @param aggregatorClass Class type of the aggregator
     * @param <A> Aggregator type
     * @return True iff aggregator wasn't already registered
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    <A extends Writable> boolean registerPersistentAggregator(String name,
        Class<? extends Aggregator<A>> aggregatorClass) throws
        InstantiationException, IllegalAccessException;

    /**
     * Sets value of an aggregator.
     *
     * @param name Name of aggregator
     * @param value Value to set
     * @param <A> Aggregated value
     */
    <A extends Writable> void setAggregatedValue(String name, A value);
}
