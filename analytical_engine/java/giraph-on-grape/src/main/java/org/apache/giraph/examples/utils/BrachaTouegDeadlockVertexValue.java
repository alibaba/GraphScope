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

package org.apache.giraph.examples.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * Vertex value used for the Bracha-Toueg Dealock algorithm
 */
public class BrachaTouegDeadlockVertexValue implements Writable {
  /** Invalid ID */
  public static final Long INVALID_ID = Long.valueOf(-1);

  /** Vertex is free from deadlock */
  private boolean isFree;
  /** Vertex was notified */
  private boolean isNotified;
  /**
   * Active requests which need to be satisfied to free the node.
   * Tha hash map is needed to handle the N-out-of-M semantics. The first
   * parameter identifies the TAG of the request, the second identifies the
   * vertex id to which an edge with points. All the eequests (edges) with the
   * same TAG need to be satisfied to consider the vertex free.
   */
  private HashMap<Long, ArrayList<Long>> requests;
  /**
   * Structure containing the messages awaited from the other vertices.
   * The algorithm guarantees that the vertex will not wait for two different
   * messages from the same vertex.
   */
  private HashMap<Long, Long> waitingList;
  /** IDs of the parents of this vertex */
  private ArrayList<Long> parents;
  /** id to which the ACK message needs to be sent for the first GRANT message
      that releases the node; -1 identifies an empty ackId */
  private Long idWithInHoldAck;
  /**
   * id to which the DONE message needs to be sent for the first NOTICE
   * message received; -1 identifies an empty ackId
   */
  private Long idWithInHoldDone;

  /**
   * Default constructor
   */
  public BrachaTouegDeadlockVertexValue() {
    this(new HashMap<Long, ArrayList<Long>>());
  }

  /**
   * Parametrized constructor
   *
   * @param requests number of requests needed to consider the node free
   */
  public BrachaTouegDeadlockVertexValue(
    HashMap<Long, ArrayList<Long>> requests) {

    this.isFree = false;
    this.isNotified = false;
    this.requests = requests;
    this.waitingList = new HashMap<Long, Long>();
    this.parents = new ArrayList<Long>();
    this.idWithInHoldAck = INVALID_ID;
    this.idWithInHoldDone = INVALID_ID;
  }

  // Serialization functions -----------------------------------------------

  @Override
  public void readFields(DataInput input) throws IOException {
    int sz;

    this.isFree = input.readBoolean();
    this.isNotified = input.readBoolean();

    sz = input.readInt();
    for (int i = 0; i < sz; ++i) {
      ArrayList<Long> targets = new ArrayList<Long>();
      Long tag = input.readLong();
      int sw = input.readInt();

      for (int j = 0; j < sw; ++j) {
        Long target = input.readLong();

        targets.add(target);
      }

      this.requests.put(tag, targets);
    }

    sz = input.readInt();
    for (int i = 0; i < sz; ++i) {
      Long key = input.readLong();
      Long value = input.readLong();

      this.waitingList.put(key, value);
    }

    sz = input.readInt();
    for (int i = 0; i < sz; ++i) {
      this.parents.add(Long.valueOf(input.readLong()));
    }

    this.idWithInHoldAck  = input.readLong();
    this.idWithInHoldDone = input.readLong();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    int sz;

    output.writeBoolean(this.isFree);
    output.writeBoolean(this.isNotified);

    sz = this.requests.size();
    output.writeInt(sz);
    for (Map.Entry<Long, ArrayList<Long>> entry : this.requests.entrySet()) {
      ArrayList<Long> targets;

      output.writeLong(entry.getKey());
      targets = entry.getValue();
      sz = targets.size();
      output.writeInt(sz);
      for (Long target : targets) {
        output.writeLong(target);
      }
    }

    sz = this.waitingList.size();
    output.writeInt(sz);
    for (Map.Entry<Long, Long> entry : this.waitingList.entrySet()) {
      output.writeLong(entry.getKey());
      output.writeLong(entry.getValue());
    }

    sz = this.parents.size();
    output.writeInt(sz);
    for (int i = 0; i < sz; ++i) {
      output.writeLong(this.parents.get(i));
    }

    output.writeLong(this.idWithInHoldAck);
    output.writeLong(this.idWithInHoldDone);
  }

  // Accessors -------------------------------------------------------------

  /**
   * @return true if free, false otherwise
   */
  public boolean isFree() {
    return this.isFree;
  }

  /**
   * the vertex is free from deadlocks
   */
  public void setFree() {
    this.isFree = true;
  }

  /**
   * @return true if the vertex was notified, false otherwise
   */
  public boolean isNotified() {
    return this.isNotified;
  }

  /**
   * the vertex got a notification
   */
  public void setNotified() {
    this.isNotified = true;
  }

  /**
   * @return false if no pending requests have to be still processed to
   *         continue the computation
   */
  public boolean hasPendingRequests() {
    boolean withPendingRequests = true;

    if (this.requests.isEmpty()) {
      withPendingRequests = false;
    }

    for (Map.Entry<Long, ArrayList<Long>> request : this.requests.entrySet()) {
      ArrayList<Long> targets = request.getValue();

      if (targets.size() == 0) {
        withPendingRequests = false;
      }
    }
    return withPendingRequests;
  }

  /**
   * remove the expected request from the edge on which the message arrived
   *
   * @param tag       tag of the edge
   * @param targetId  target Id to which the edge points
   */
  public void removeRequest(LongWritable tag, LongWritable targetId) {
    Long l = Long.valueOf(tag.get());
    ArrayList<Long> targets = this.requests.get(l);

    if (targets.contains(targetId.get())) {
      targets.remove(Long.valueOf(targetId.get()));
    }
  }

  /**
   * This function retrieves the number of pending requests for the specified
   * tag. Because of the N-out-of-M semantic, each time a GRANT is received
   * on an edge, the number of requests is reduced for the tag which the edge
   * is part of.
   *
   * @param tag   tag related to the requests to be verified
   * @return number of requests pending for the tag provided
   */
  public int getNumOfRequests(LongWritable tag) {
    Long l = Long.valueOf(tag.get());
    ArrayList<Long> targets = this.requests.get(l);

    return targets.size();
  }

  /**
   * Add a new message that must be expected by the node
   *
   * @param  id        ID of the node from which the messages is expected
   * @param  type      type of message that is awaited
   */
  public void waitForMessage(Long id, Long type) {
    // waiting list should not contain two messages for the same node
    assert waitingList.get(id) == null;
    waitingList.put(id, type);
  }

  /**
   * Each time a message is received, it has to be removed from the queue
   * that keeps track of the waited messages.
   *
   * @param  id        ID of the node from which the messages is expected
   * @param  type      type of message that is awaited
   */
  public void receivedMessage(Long id, Long type) {
    long typel;

    assert waitingList.get(id) != null;
    typel = waitingList.get(id).longValue();
    assert typel > 0;
    waitingList.remove(id);
  }

  /**
   * @param  type       type of message to check
   * @return boolean    true if waiting the message type, false otherwise
   */
  public boolean isWaitingForMessage(Long type) {
    for (Map.Entry<Long, Long> entry : waitingList.entrySet()) {
      long typel = entry.getValue().longValue();
      if ((typel & type) > 0) {
        return true;
      }
    }

    return false;
  }

  /**
   * add a parent id into the list of parents kept at the vertex side
   * @param parentId    vertex id of the parent
   */
  public void addParent(Long parentId) {
    this.parents.add(parentId);
  }

  /**
   * @return list of parent IDs collected
   */
  public ArrayList<Long> getParents() {
    return this.parents;
  }

  /**
   * @return the id waiting for an ACK message
   */
  public Long getIdWithInHoldAck() {
    return this.idWithInHoldAck;
  }

  /**
   * @param id the id to set
   */
  public void setIdWithInHoldAck(Long id) {
    this.idWithInHoldAck = id;
  }

  /**
   * @return the id waiting for an DONE message
   */
  public Long getIdWithInHoldDone() {
    return idWithInHoldDone;
  }

  /**
   * @param doneId the id to set
   */
  public void setIdWithInHoldDone(Long doneId) {
    this.idWithInHoldDone = doneId;
  }

  @Override
  public String toString() {
    return "isFree=" + Boolean.toString(isFree);
  }
}
