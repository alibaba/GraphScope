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

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.Map.Entry;

/**
 * An AggregatorWriter is used to export Aggregators during or at the end of each computation. It
 * runs on the master and it's called at the end of each superstep. The special signal {@link
 * AggregatorWriter#LAST_SUPERSTEP} is passed to {@link AggregatorWriter#writeAggregator(Iterable,
 * long)} as the superstep value to signal the end of computation.
 */
public interface AggregatorWriter extends ImmutableClassesGiraphConfigurable {

    /**
     * Signal for last superstep
     */
    int LAST_SUPERSTEP = -1;

    /**
     * The method is called at the initialization of the AggregatorWriter. More precisely, the
     * aggregatorWriter is initialized each time a new master is elected.
     *
     * @param context            Mapper Context where the master is running on
     * @param applicationAttempt ID of the applicationAttempt, used to disambiguate aggregator
     *                           writes for different attempts
     * @throws IOException
     */
    @SuppressWarnings("rawtypes")
    void initialize(Context context, long applicationAttempt) throws IOException;

    /**
     * The method is called at the end of each superstep. The user might decide whether to write the
     * aggregators values for the current superstep. For the last superstep, {@link
     * AggregatorWriter#LAST_SUPERSTEP} is passed.
     *
     * @param aggregatorMap Map from aggregator name to aggregator value
     * @param superstep     Current superstep
     * @throws IOException
     */
    void writeAggregator(Iterable<Entry<String, Writable>> aggregatorMap, long superstep)
            throws IOException;

    /**
     * The method is called at the end of a successful computation. The method is not called when
     * the job fails and a new master is elected. For this reason it's advised to flush data at the
     * end of {@link AggregatorWriter#writeAggregator(Iterable, long)}.
     *
     * @throws IOException
     */
    void close() throws IOException;
}
