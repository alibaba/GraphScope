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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.reducers.ReduceSameTypeOperation;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

/**
 * Translates aggregation operation to reduce operations.
 *
 * @param <A> Aggregation object type
 */
public class AggregatorReduceOperation<A extends Writable>
    extends ReduceSameTypeOperation<A> implements GiraphConfigurationSettable {
    /** Aggregator class */
    private Class<? extends Aggregator<A>> aggregatorClass;
    /** Aggregator */
    private Aggregator<A> aggregator;
    /** Configuration */
    private ImmutableClassesGiraphConfiguration<?, ?, ?> conf;

    /** Constructor */
    public AggregatorReduceOperation() {
    }

    /**
     * Constructor
     * @param aggregatorClass Aggregator class
     * @param conf Configuration
     */
    public AggregatorReduceOperation(
        Class<? extends Aggregator<A>> aggregatorClass,
        ImmutableClassesGiraphConfiguration<?, ?, ?> conf) {
        this.aggregatorClass = aggregatorClass;
        this.conf = conf;
        initAggregator();
    }

    /** Initialize aggregator */
    private void initAggregator() {
        aggregator = ReflectionUtils.newInstance(aggregatorClass, conf);
        aggregator.setAggregatedValue(null);
    }

    @Override
    public A createInitialValue() {
        A agg = aggregator.createInitialValue();
        if (agg == null) {
            throw new IllegalStateException(
                "Aggregators initial value must not be null, but is for " +
                    aggregator);
        }
        return agg;
    }

    /**
     * Creates copy of this object
     * @return copy
     */
    public AggregatorReduceOperation<A> createCopy() {
        return new AggregatorReduceOperation<>(aggregatorClass, conf);
    }

    public Class<? extends Aggregator<A>> getAggregatorClass() {
        return aggregatorClass;
    }

    @Override
    public synchronized A reduce(A curValue, A valueToReduce) {
        aggregator.setAggregatedValue(curValue);
        aggregator.aggregate(valueToReduce);
        A aggregated = aggregator.getAggregatedValue();
        aggregator.setAggregatedValue(null);
        return aggregated;
    }

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeClass(aggregatorClass, out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        aggregatorClass = WritableUtils.readClass(in);
        initAggregator();
    }


}
