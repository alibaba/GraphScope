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
package org.apache.giraph.examples.scc;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * This master compute keeps track of what phase is being currently executed by
 * the Strongly Connected Components computation. The phases comprehend the
 * following: 1 - Transpose (comprehends 2 supersteps, one to propagate parent
 * vertices ids and another one to store them by their respective children) 2 -
 * Trimming (this phase can happen multiple times) 3 - Forward Traversal 4 -
 * Backward Traversal
 */
public class SccPhaseMasterCompute extends DefaultMasterCompute {

  /**
   * Aggregator that stores the current phase
   */
  public static final String PHASE = "scccompute.phase";

  /**
   * Flags whether a new maximum was found in the Forward Traversal phase
   */
  public static final String NEW_MAXIMUM = "scccompute.max";

  /**
   * Flags whether a vertex converged in the Backward Traversal phase
   */
  public static final String CONVERGED = "scccompute.converged";

  /**
   * Enumerates the possible phases of the algorithm.
   */
  public enum Phases {
    /** Tranpose and Trimming phases **/
    TRANSPOSE, TRIMMING,
    /** Maximum id propagation **/
    FORWARD_TRAVERSAL,
    /** Vertex convergence in SCC **/
    BACKWARD_TRAVERSAL_START, BACKWARD_TRAVERSAL_REST
  };

  @Override
  public void initialize() throws InstantiationException,
      IllegalAccessException {
    registerPersistentAggregator(PHASE, IntOverwriteAggregator.class);
    registerAggregator(NEW_MAXIMUM, BooleanOrAggregator.class);
    registerAggregator(CONVERGED, BooleanOrAggregator.class);
  }

  @Override
  public void compute() {
    if (getSuperstep() == 0) {
      setPhase(Phases.TRANSPOSE);
    } else {
      Phases currPhase = getPhase();
      switch (currPhase) {
      case TRANSPOSE:
        setPhase(Phases.TRIMMING);
        break;
      case TRIMMING :
        setPhase(Phases.FORWARD_TRAVERSAL);
        break;
      case FORWARD_TRAVERSAL :
        BooleanWritable newMaxFound = getAggregatedValue(NEW_MAXIMUM);
        // If no new maximum value was found it means the propagation
        // converged, so we can move to the next phase
        if (!newMaxFound.get()) {
          setPhase(Phases.BACKWARD_TRAVERSAL_START);
        }
        break;
      case BACKWARD_TRAVERSAL_START :
        setPhase(Phases.BACKWARD_TRAVERSAL_REST);
        break;
      case BACKWARD_TRAVERSAL_REST :
        BooleanWritable converged = getAggregatedValue(CONVERGED);
        if (!converged.get()) {
          setPhase(Phases.TRANSPOSE);
        }
        break;
      default :
        break;
      }
    }
  }

  /**
   * Sets the next phase of the algorithm.
   * @param phase
   *          Next phase.
   */
  private void setPhase(Phases phase) {
    setAggregatedValue(PHASE, new IntWritable(phase.ordinal()));
  }

  /**
   * Get current phase.
   * @return Current phase as enumerator.
   */
  private Phases getPhase() {
    IntWritable phaseInt = getAggregatedValue(PHASE);
    return getPhase(phaseInt);
  }

  /**
   * Helper function to convert from internal aggregated value to a Phases
   * enumerator.
   * @param phaseInt
   *          An integer that matches a position in the Phases enumerator.
   * @return A Phases' item for the given position.
   */
  public static Phases getPhase(IntWritable phaseInt) {
    return Phases.values()[phaseInt.get()];
  }

}
