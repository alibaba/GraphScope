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
 * Abstract class for {@link Aggregator}. Implements get value, set value and reset methods and has
 * internal value object.
 *
 * @param <A> Aggregated value
 */
public abstract class BasicAggregator<A extends Writable> implements Aggregator<A> {

    /**
     * Internal value
     */
    private A value;

    /**
     * Default constructor. Creates new value object and resets the aggregator
     */
    public BasicAggregator() {
        value = createInitialValue();
    }

    /**
     * Constructor
     *
     * @param initialValue initial value
     */
    public BasicAggregator(A initialValue) {
        value = initialValue;
    }

    @Override
    public A getAggregatedValue() {
        return value;
    }

    @Override
    public void setAggregatedValue(A value) {
        this.value = value;
    }

    @Override
    public void reset() {
        value = createInitialValue();
    }
}
