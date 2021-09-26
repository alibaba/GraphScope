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

package org.apache.giraph.examples;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Master compute associated with {@link RandomWalkComputation}. It handles dangling nodes.
 */
public class RandomWalkVertexMasterCompute extends DefaultMasterCompute {

    /**
     * threshold for the L1 norm of the state vector difference
     */
    static final double CONVERGENCE_THRESHOLD = 0.00001;

    /**
     * logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(RandomWalkVertexMasterCompute.class);

    @Override
    public void compute() {
        double danglingContribution =
                this.<DoubleWritable>getAggregatedValue(
                                RandomWalkComputation.CUMULATIVE_DANGLING_PROBABILITY)
                        .get();
        double cumulativeProbability =
                this.<DoubleWritable>getAggregatedValue(
                                RandomWalkComputation.CUMULATIVE_PROBABILITY)
                        .get();
        double l1NormOfStateDiff =
                this.<DoubleWritable>getAggregatedValue(
                                RandomWalkComputation.L1_NORM_OF_PROBABILITY_DIFFERENCE)
                        .get();
        long numDanglingVertices =
                this.<LongWritable>getAggregatedValue(RandomWalkComputation.NUM_DANGLING_VERTICES)
                        .get();

        LOG.info(
                "[Superstep "
                        + getSuperstep()
                        + "] Dangling contribution = "
                        + danglingContribution
                        + ", number of dangling vertices = "
                        + numDanglingVertices
                        + ", cumulative probability = "
                        + cumulativeProbability
                        + ", L1 Norm of state vector difference = "
                        + l1NormOfStateDiff);

        // Convergence check: halt once the L1 norm of the difference between the
        // state vectors fall below the threshold
        if (getSuperstep() > 1 && l1NormOfStateDiff < CONVERGENCE_THRESHOLD) {
            haltComputation();
        }
    }

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        registerAggregator(RandomWalkComputation.NUM_DANGLING_VERTICES, LongSumAggregator.class);
        registerAggregator(
                RandomWalkComputation.CUMULATIVE_DANGLING_PROBABILITY, DoubleSumAggregator.class);
        registerAggregator(RandomWalkComputation.CUMULATIVE_PROBABILITY, DoubleSumAggregator.class);
        registerAggregator(
                RandomWalkComputation.L1_NORM_OF_PROBABILITY_DIFFERENCE, DoubleSumAggregator.class);
    }
}
