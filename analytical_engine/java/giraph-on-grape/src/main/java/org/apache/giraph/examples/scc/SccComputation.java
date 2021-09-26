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

import static org.apache.giraph.examples.scc.SccPhaseMasterCompute.PHASE;
import static org.apache.giraph.examples.scc.SccPhaseMasterCompute.NEW_MAXIMUM;
import static org.apache.giraph.examples.scc.SccPhaseMasterCompute.CONVERGED;

import java.io.IOException;

import org.apache.giraph.Algorithm;
import org.apache.giraph.examples.scc.SccPhaseMasterCompute.Phases;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Finds strongly connected components of the graph.
 */
@Algorithm(name = "Strongly Connected Components",
           description = "Finds strongly connected components of the graph")
public class SccComputation extends
    BasicComputation<LongWritable, SccVertexValue, NullWritable, LongWritable> {

  /**
   * Current phase of the computation as defined in SccPhaseMasterCompute
   */
  private Phases currPhase;

  /**
   * Reusable object to encapsulate message value, in order to avoid
   * creating a new instance every time a message is sent.
   */
  private LongWritable messageValue = new LongWritable();

  /**
   * Reusable object to encapsulate a parent vertex id.
   */
  private LongWritable parentId = new LongWritable();

  @Override
  public void preSuperstep() {
    IntWritable phaseInt = getAggregatedValue(PHASE);
    currPhase = SccPhaseMasterCompute.getPhase(phaseInt);
  }

  @Override
  public void compute(
      Vertex<LongWritable, SccVertexValue, NullWritable> vertex,
      Iterable<LongWritable> messages) throws IOException {

    SccVertexValue vertexValue = vertex.getValue();

    if (!vertexValue.isActive()) {
      vertex.voteToHalt();
      return;
    }

    switch (currPhase) {
    case TRANSPOSE :
      vertexValue.clearParents();
      sendMessageToAllEdges(vertex, vertex.getId());
      break;
    case TRIMMING :
      trim(vertex, messages);
      break;
    case FORWARD_TRAVERSAL :
      forwardTraversal(vertex, messages);
      break;
    case BACKWARD_TRAVERSAL_START :
      backwardTraversalStart(vertex);
      break;
    case BACKWARD_TRAVERSAL_REST :
      backwardTraversalRest(vertex, messages);
      break;
    default :
      break;
    }

  }

  /**
   * Creates list of parents based on the received ids and halts the vertices
   * that don't have any parent or outgoing edge, hence, they can't be
   * part of an SCC.
   * @param vertex Current vertex.
   * @param messages Received ids from the Transpose phase.
   */
  private void trim(Vertex<LongWritable, SccVertexValue, NullWritable> vertex,
                    Iterable<LongWritable> messages) {
    SccVertexValue vertexValue = vertex.getValue();
    // Keep the ids of the parent nodes to allow for backwards traversal
    for (LongWritable parent : messages) {
      vertexValue.addParent(parent.get());
    }
    // If this node doesn't have any parents or outgoing edges,
    // it can't be part of an SCC
    vertexValue.set(vertex.getId().get());
    if (vertex.getNumEdges() == 0 || vertexValue.getParents() == null) {
      vertexValue.deactivate();
    } else {
      messageValue.set(vertexValue.get());
      sendMessageToAllEdges(vertex, messageValue);
    }
  }

  /**
   * Traverse the graph through outgoing edges and keep the maximum vertex
   * value.
   * If a new maximum value is found, propagate it until convergence.
   * @param vertex Current vertex.
   * @param messages Received values from neighbor vertices.
   */
  private void forwardTraversal(
      Vertex<LongWritable, SccVertexValue, NullWritable> vertex,
      Iterable<LongWritable> messages) {
    SccVertexValue vertexValue = vertex.getValue();
    boolean changed = setMaxValue(vertexValue, messages);
    if (changed) {
      messageValue.set(vertexValue.get());
      sendMessageToAllEdges(vertex, messageValue);
      aggregate(NEW_MAXIMUM, new BooleanWritable(true));
    }
  }

  /**
   * Traverse the transposed graph and keep the maximum vertex value.
   * @param vertex Current vertex.
   */
  private void backwardTraversalStart(
      Vertex<LongWritable, SccVertexValue, NullWritable> vertex) {
    SccVertexValue vertexValue = vertex.getValue();
    if (vertexValue.get() == vertex.getId().get()) {
      messageValue.set(vertexValue.get());
      sendMessageToAllParents(vertex, messageValue);
    }
  }

  /**
   * Traverse the transposed graph and keep the maximum vertex value.
   * @param vertex Current vertex.
   * @param messages Received values from children vertices.
   */
  private void backwardTraversalRest(
      Vertex<LongWritable, SccVertexValue, NullWritable> vertex,
      Iterable<LongWritable> messages) {
    SccVertexValue vertexValue = vertex.getValue();
    for (LongWritable m : messages) {
      if (vertexValue.get() == m.get()) {
        sendMessageToAllParents(vertex, m);
        aggregate(CONVERGED, new BooleanWritable(true));
        vertexValue.deactivate();
        vertex.voteToHalt();
        break;
      }
    }
  }

  /**
   * Compares the messages values with the current vertex value and finds
   * the maximum.
   * If the maximum value is different from the vertex value, makes it the
   * new vertex value and returns true, otherwise, returns false.
   * @param vertexValue Current vertex value.
   * @param messages Messages containing neighbors' vertex values.
   * @return True if a new maximum was found, otherwise, returns false.
   */
  private boolean setMaxValue(SccVertexValue vertexValue,
                              Iterable<LongWritable> messages) {
    boolean changed = false;
    for (LongWritable m : messages) {
      if (vertexValue.get() < m.get()) {
        vertexValue.set(m.get());
        changed = true;
      }
    }
    return changed;
  }


  /**
   * Send message to all parents.
   * @param vertex Current vertex.
   * @param message Message to be sent.
   */
  private void sendMessageToAllParents(
      Vertex<LongWritable, SccVertexValue, NullWritable> vertex,
      LongWritable message) {
    for (Long id : vertex.getValue().getParents()) {
      parentId.set(id);
      sendMessage(parentId, message);
    }
  }
}
