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
package org.apache.giraph.worker;

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Class for delegating WorkerAggregatorUsage and WorkerGlobalCommUsage methods to corresponding
 * interface.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public abstract class WorkerAggregatorDelegator<
                I extends WritableComparable, V extends Writable, E extends Writable>
        extends DefaultImmutableClassesGiraphConfigurable<I, V, E> {
    //    implements WorkerAggregatorUsage, WorkerGlobalCommUsage {

    /** Worker aggregator usage */
    //    private WorkerGlobalCommUsage workerGlobalCommUsage;

    /**
     * Set worker global communication usage
     *
     * @param workerGlobalCommUsage Worker global communication usage
     */
    //    public void setWorkerGlobalCommUsage(
    //        WorkerGlobalCommUsage workerGlobalCommUsage) {
    //        this.workerGlobalCommUsage = workerGlobalCommUsage;
    //    }
    //
    //    @Override
    //    public final void reduce(String name, Object value) {
    //        workerGlobalCommUsage.reduce(name, value);
    //    }
    //
    //    @Override
    //    public void reduceMerge(String name, Writable value) {
    //        workerGlobalCommUsage.reduceMerge(name, value);
    //    }
    //
    //    @Override
    //    public final <B extends Writable> B getBroadcast(String name) {
    //        return workerGlobalCommUsage.getBroadcast(name);
    //    }
    //
    //    @Override
    //    public final <A extends Writable> void aggregate(String name, A value) {
    //        reduce(name, value);
    //    }
    //
    //    @Override
    //    public <A extends Writable> A getAggregatedValue(String name) {
    //        AggregatorBroadcast<A> broadcast = workerGlobalCommUsage.getBroadcast(name);
    //        return broadcast.getValue();
    //    }
}
