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

import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Vertex value for the Strongly Connected Components algorithm. It keeps track
 * of the parents of the vertex in order to traverse the graph backwards.
 */
public class SccVertexValue implements Writable {

  /** Vertex's parents **/
  private LongArrayList parents;

  /** Current vertex value **/
  private long value = Long.MIN_VALUE;

  /** Indicates whether the vertex was trimmed, hence,
   * it can't be part of the computation anymore.
   */
  private boolean active = true;

  /**
   * Public constructor required for serialization.
   */
  public SccVertexValue() {
  }

  /**
   * Constructor
   * @param value Initial value for this vertex.
   */
  public SccVertexValue(long value) {
    this.value = value;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value = in.readLong();

    int size = in.readInt();
    if (size != 0) {
      for (int i = 0; i < size; i++) {
        addParent(in.readLong());
      }
    }

    active = in.readBoolean();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(value);

    int size = parents == null ? 0 : parents.size();
    out.writeInt(size);
    if (size != 0) {
      for (long incomingId : parents) {
        out.writeLong(incomingId);
      }
    }

    out.writeBoolean(active);
  }

  /**
   * Returns the list of parent vertices, i.e., vertices that are at the other
   * end of incoming edges. If the vertex doesn't have any incoming edge, it
   * returns null.
   * @return List of the vertex's parents.
   */
  public LongArrayList getParents() {
    return parents;
  }

  /**
   * Adds a vertex id to the list of parent vertices.
   * @param vertexId It of the parent vertex.
   */
  public void addParent(long vertexId) {
    // Initialize the list of parent vertices only when one attempts to add
    // the first item, so we save some memory on vertices that have no incoming
    // edges
    if (parents == null) {
      parents = new LongArrayList();
    }
    parents.add(vertexId);
  }

  /**
   * Clear parents list.
   */
  public void clearParents() {
    parents = null;
  }

  /**
   * Sets the vertex value. At the end of the SCC computation, vertices with the
   * same vertex value are part of the same component.
   * @param value Vertex value.
   */
  public void set(long value) {
    this.value = value;
  }

  /**
   * Returns the vertex value. At the end of the SCC computation, vertices with
   * the same vertex value are part of the same component.
   * @return Current vertex value.
   */
  public long get() {
    return value;
  }

  /**
   * Remove this vertex from the computation.
   */
  public void deactivate() {
    this.active = false;
  }

  /**
   * Indicates whether the vertex was removed in a Trimming phase.
   * @return True if the vertex was trimmed, otherwise, return false.
   */
  public boolean isActive() {
    return active;
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }

}
