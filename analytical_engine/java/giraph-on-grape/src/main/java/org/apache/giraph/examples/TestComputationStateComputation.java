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

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Vertex to test the local variables in Computation, and pre/postSuperstep
 * methods
 */
public class TestComputationStateComputation extends BasicComputation<
    LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
  /** How many compute threads to use in the test */
  public static final int NUM_COMPUTE_THREADS = 10;
  /** How many vertices to create for the test */
  public static final int NUM_VERTICES = 100;
  /** How many partitions to have */
  public static final int NUM_PARTITIONS = 25;

  /**
   * The counter should hold the number of vertices in this partition,
   * plus the current superstep
   */
  private long counter;

  @Override
  public void compute(
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
      Iterable<DoubleWritable> messages) throws IOException {
    counter++;
    if (getSuperstep() > 5) {
      vertex.voteToHalt();
    }
  }

  @Override
  public void preSuperstep() {
    counter =
      ((TestComputationStateWorkerContext) getWorkerContext()).superstepCounter;
  }

  @Override
  public void postSuperstep() {
    ((TestComputationStateWorkerContext) getWorkerContext()).totalCounter
        .addAndGet(counter);
  }

  /**
   * WorkerContext for TestComputationState
   */
  public static class TestComputationStateWorkerContext extends
      DefaultWorkerContext {
    /** Current superstep */
    private long superstepCounter;
    /**
     * This counter should hold the sum of Computation's counters
     */
    private AtomicLong totalCounter;

    @Override
    public void preSuperstep() {
      superstepCounter = getSuperstep();
      totalCounter = new AtomicLong(0);
    }

    @Override
    public void postSuperstep() {
      assertEquals(totalCounter.get(),
          NUM_COMPUTE_THREADS * superstepCounter + getTotalNumVertices());
    }
  }

  /**
   * Throws exception if values are not equal.
   *
   * @param expected Expected value
   * @param actual   Actual value
   */
  private static void assertEquals(long expected, long actual) {
    if (expected != actual) {
      throw new RuntimeException("expected: " + expected +
          ", actual: " + actual);
    }
  }
}
