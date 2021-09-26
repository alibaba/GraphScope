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

import com.google.common.base.Preconditions;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MathUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Executes "RandomWalkWithRestart", a random walk on the graph which is biased
 * towards a source vertex. The resulting probabilities of staying at a given
 * vertex can be interpreted as a measure of proximity to the source vertex.
 */
public class RandomWalkWithRestartComputation
    extends RandomWalkComputation<DoubleWritable> {

  /** Configuration parameter for the source vertex */
  static final String SOURCE_VERTEX = RandomWalkWithRestartComputation.class
      .getName() + ".sourceVertex";

  /**
   * Checks whether the currently executed vertex is the source vertex
   * @param vertex Vertex
   * @return is the currently executed vertex the source vertex?
   */
  private boolean isSourceVertex(Vertex<LongWritable, ?, ?> vertex) {
    return ((RandomWalkWorkerContext) getWorkerContext()).isSource(
        vertex.getId().get());
  }

  /**
   * Returns the number of source vertices.
   * @return The number of source vertices.
   */
  private int numSourceVertices() {
    return ((RandomWalkWorkerContext) getWorkerContext()).numSources();
  }

  @Override
  protected double transitionProbability(
      Vertex<LongWritable, DoubleWritable, DoubleWritable>
          vertex,
      double stateProbability, Edge<LongWritable, DoubleWritable> edge) {
    return stateProbability * edge.getValue().get();
  }

  @Override
  protected double recompute(
      Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex,
      Iterable<DoubleWritable> transitionProbabilities,
      double teleportationProbability) {
    int numSourceVertices = numSourceVertices();
    Preconditions.checkState(numSourceVertices > 0, "No source vertex found");

    double stateProbability = MathUtils.sum(transitionProbabilities);
    // Add the contribution of dangling nodes (weakly preferential
    // implementation: dangling nodes redistribute uniformly)
    stateProbability += getDanglingProbability() / getTotalNumVertices();
    // The random walk might teleport back to one of the source vertexes
    stateProbability *= 1 - teleportationProbability;
    if (isSourceVertex(vertex)) {
      stateProbability += teleportationProbability / numSourceVertices;
    }
    return stateProbability;
  }
}
