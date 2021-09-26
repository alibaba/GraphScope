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

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.slf4j.LoggerFactory;

/**
 * An example that simply uses its id, value, and edges to compute new data
 * every iteration to verify that messages are sent and received at the
 * appropriate location and superstep.
 */
public class VerifyMessage {
  /**
   * Message that will be sent in {@link VerifyMessageComputation}.
   */
  public static class VerifiableMessage implements Writable {
    /** Superstep sent on */
    private long superstep;
    /** Source vertex id */
    private long sourceVertexId;
    /** Value */
    private float value;

    /**
     * Default constructor used with reflection.
     */
    public VerifiableMessage() { }

    /**
     * Constructor with verifiable arguments.
     * @param superstep Superstep this message was created on.
     * @param sourceVertexId Who send this message.
     * @param value A value associated with this message.
     */
    public VerifiableMessage(
        long superstep, long sourceVertexId, float value) {
      this.superstep = superstep;
      this.sourceVertexId = sourceVertexId;
      this.value = value;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
      superstep = input.readLong();
      sourceVertexId = input.readLong();
      value = input.readFloat();
    }

    @Override
    public void write(DataOutput output) throws IOException {
      output.writeLong(superstep);
      output.writeLong(sourceVertexId);
      output.writeFloat(value);
    }

    @Override
    public String toString() {
      return "(superstep=" + superstep + ",sourceVertexId=" +
          sourceVertexId + ",value=" + value + ")";
    }
  }

  /**
   * Send and verify messages.
   */
  public static class VerifyMessageComputation extends
      BasicComputation<LongWritable, IntWritable, FloatWritable,
          VerifiableMessage> {
    /** Dynamically set number of SUPERSTEPS */
    public static final String SUPERSTEP_COUNT =
        "verifyMessageVertex.superstepCount";
    /** User can access this after the application finishes if local */
    private static long FINAL_SUM;
    /** Number of SUPERSTEPS to run (6 by default) */
    private static int SUPERSTEPS = 6;
    /** Class logger */
    private static Logger LOG =
        LoggerFactory.getLogger(VerifyMessageComputation.class);

    public static long getFinalSum() {
      return FINAL_SUM;
    }

    /**
     * Worker context used with {@link VerifyMessageComputation}.
     */
    public static class VerifyMessageVertexWorkerContext extends
        WorkerContext {
      @Override
      public void preApplication() throws InstantiationException,
      IllegalAccessException {
        SUPERSTEPS = getContext().getConfiguration().getInt(
            SUPERSTEP_COUNT, SUPERSTEPS);
      }

      @Override
      public void postApplication() {
        LongWritable sumAggregatorValue =
            getAggregatedValue(LongSumAggregator.class.getName());
        FINAL_SUM = sumAggregatorValue.get();
      }

      @Override
      public void preSuperstep() {
      }

      @Override
      public void postSuperstep() { }
    }

    @Override
    public void compute(
        Vertex<LongWritable, IntWritable, FloatWritable> vertex,
        Iterable<VerifiableMessage> messages) throws IOException {
      String sumAggregatorName = LongSumAggregator.class.getName();
      if (getSuperstep() > SUPERSTEPS) {
        vertex.voteToHalt();
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("compute: " + getAggregatedValue(sumAggregatorName));
      }
      aggregate(sumAggregatorName, new LongWritable(vertex.getId().get()));
      if (LOG.isDebugEnabled()) {
        LOG.debug("compute: sum = " +
            this.<LongWritable>getAggregatedValue(sumAggregatorName).get() +
            " for vertex " + vertex.getId());
      }
      float msgValue = 0.0f;
      for (VerifiableMessage message : messages) {
        msgValue += message.value;
        if (LOG.isDebugEnabled()) {
          LOG.debug("compute: got msg = " + message +
              " for vertex id " + vertex.getId() +
              ", vertex value " + vertex.getValue() +
              " on superstep " + getSuperstep());
        }
        if (message.superstep != getSuperstep() - 1) {
          throw new IllegalStateException(
              "compute: Impossible to not get a messsage from " +
                  "the previous superstep, current superstep = " +
                  getSuperstep());
        }
        if ((message.sourceVertexId != vertex.getId().get() - 1) &&
            (vertex.getId().get() != 0)) {
          throw new IllegalStateException(
              "compute: Impossible that this message didn't come " +
                  "from the previous vertex and came from " +
                  message.sourceVertexId);
        }
      }
      int vertexValue = vertex.getValue().get();
      vertex.setValue(new IntWritable(vertexValue + (int) msgValue));
      if (LOG.isDebugEnabled()) {
        LOG.debug("compute: vertex " + vertex.getId() +
            " has value " + vertex.getValue() +
            " on superstep " + getSuperstep());
      }
      for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
        FloatWritable newEdgeValue = new FloatWritable(
            edge.getValue().get() + (float) vertexValue);
        Edge<LongWritable, FloatWritable> newEdge =
            EdgeFactory.create(edge.getTargetVertexId(), newEdgeValue);
        if (LOG.isDebugEnabled()) {
          LOG.debug("compute: vertex " + vertex.getId() +
              " sending edgeValue " + edge.getValue() +
              " vertexValue " + vertexValue +
              " total " + newEdgeValue +
              " to vertex " + edge.getTargetVertexId() +
              " on superstep " + getSuperstep());
        }
        vertex.addEdge(newEdge);
        sendMessage(edge.getTargetVertexId(),
            new VerifiableMessage(
                getSuperstep(), vertex.getId().get(), newEdgeValue.get()));
      }
    }
  }

  /**
   * Master compute associated with {@link VerifyMessageComputation}.
   * It registers required aggregators.
   */
  public static class VerifyMessageMasterCompute extends
      DefaultMasterCompute {
    @Override
    public void initialize() throws InstantiationException,
        IllegalAccessException {
      registerAggregator(LongSumAggregator.class.getName(),
          LongSumAggregator.class);
    }
  }
}
