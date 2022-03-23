/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.giraph.worker;

import com.alibaba.graphscope.graph.AggregatorManager;

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.master.AggregatorBroadcast;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public abstract class WorkerAggregatorDelegator<
                I extends WritableComparable, V extends Writable, E extends Writable>
        extends DefaultImmutableClassesGiraphConfigurable<I, V, E>
        implements WorkerAggregatorUsage, WorkerGlobalCommUsage {
    private static Logger logger = LoggerFactory.getLogger(WorkerAggregatorDelegator.class);

    private AggregatorManager aggregatorManager;

    public void setAggregatorManager(AggregatorManager aggregatorManager) {
        this.aggregatorManager = aggregatorManager;
    }

    @Override
    public void reduce(String name, Object value) {
        if (Objects.isNull(aggregatorManager)) {
            logger.error("Null aggregator manager, set to a valid reference first.");
            return;
        }
        aggregatorManager.reduce(name, value);
    }

    @Override
    public void reduceMerge(String name, Writable value) {
        if (Objects.isNull(aggregatorManager)) {
            logger.error("Null aggregator manager, set to a valid reference first.");
            return;
        }
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public final <B extends Writable> B getBroadcast(String name) {
        if (Objects.isNull(aggregatorManager)) {
            logger.error("Null aggregator manager, set to a valid reference first.");
            return null;
        }
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public <A extends Writable> void aggregate(String name, A value) {
        if (Objects.isNull(aggregatorManager)) {
            logger.error("Null aggregator manager, set to a valid reference first.");
            return;
        }
        aggregatorManager.aggregate(name, value);
    }

    @Override
    public <A extends Writable> A getAggregatedValue(String name) {
        AggregatorBroadcast<A> broadcast = aggregatorManager.getAggregatedValue(name);
        return broadcast.getValue();
    }
}
