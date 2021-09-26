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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.giraph.Algorithm;
import org.apache.giraph.examples.utils.BrachaTouegDeadlockVertexValue;
import org.apache.giraph.examples.utils.BrachaTouegDeadlockMessage;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This code demonstrates the Bracha Toueg deadlock detection algorithm.
 * The Bracha Toueg algorithm is a distributed, asynchronous, centralized
 * algorithm for deadlock detection. The algorithm is executed on a snapshot of
 * a undirected graph which depicts the corresponding wait-for-graph.
 * Consequently the algorithm works on <b>directed graphs</b> but assumes
 * the possibility to communicate in both ways on all the edges.
 * This is an adaptation of the standard algorithm for Giraph/Pregel system.
 * Since the value of the vertex is dumped during checkpointing, the algorithm
 * keeps all the state of the vertex in the value.
 */
@Algorithm(
    name = "Bracha Toueg deadlock detection"
)
public class BrachaTouegDeadlockComputation
    extends BasicComputation<LongWritable, BrachaTouegDeadlockVertexValue,
    LongWritable, BrachaTouegDeadlockMessage> {

  /** The deadlock detection initiator id */
  public static final LongConfOption BRACHA_TOUEG_DL_INITIATOR_ID =
      new LongConfOption("BrachaTouegDeadlockVertex.initiatorId", 1,
          "The deadlock detection initiator id");

  /** Class logger */
  private static final Logger LOG =
      LoggerFactory.getLogger(BrachaTouegDeadlockComputation.class);

  @Override
  public void compute(
      Vertex<LongWritable, BrachaTouegDeadlockVertexValue, LongWritable> vertex,
      Iterable<BrachaTouegDeadlockMessage> messages)
      throws IOException {

    BrachaTouegDeadlockVertexValue value;
    long superstep = getSuperstep();

    if (superstep == 0) {
      /* Phase to exchange the sender vertex IDs on the incoming edges.
         It also prepares the internal state of the vertex */
      initAlgorithm(vertex);

    /* After each vertex collects the messages sent by the parents, the
       initiator node starts the algorithm by means of a NOTIFY message */
    } else if (superstep == 1) {
      /* get the value/state of the vertex */
      value = vertex.getValue();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Vertex ID " + vertex.getId() + " status is:");
        LOG.debug("\tpending requests? " + value.hasPendingRequests());
        LOG.debug("\tis free? " + value.isFree());
        LOG.debug("\tis notified? " + value.isNotified());
      }

      /* collect all the incoming senders IDs */
      for (BrachaTouegDeadlockMessage message : messages) {
        value.addParent(Long.valueOf(message.getSenderId()));
      }

      /* debugging purpose: print all the parents of the vertex */
      if (LOG.isDebugEnabled()) {
        logParents(vertex);
        if (isInitiator(vertex)) {
          LOG.debug("Vertex ID " + vertex.getId() + " start the algorithm.");
        }
      }

      if (isInitiator(vertex)) {
        /* the initiator starts the algorithm */
        notifyVertices(vertex);
      } else {
        /* The Pregel model prescribes that each node starts in the "active"
           state. In some cases the Bracha-Toueg Algorithm leaves some nodes
           untouched causing the algorithm never to end. To avoid this
           situation at algorithm initialization all the nodes except the
           initiator (which by default is active) will vote to halt so that
           the unused vertices will not produce an infinite computation. Later,
           only when required the vote will be triggered. */
        vertex.voteToHalt();
        return;
      }

      /* At this point the actual deadlock detection algorithm is started. */
    } else {
      Long ackSenderId;

      value = vertex.getValue();

      /* process all the incoming messages and act based on the type of
         message received */
      for (BrachaTouegDeadlockMessage message : messages) {
        long type = message.getType();

        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex ID " + vertex.getId() + " received: " + message);
        }

        if (type == BrachaTouegDeadlockMessage.NOTIFY) {
          handleNotifyMessage(vertex, message);
        } else if (type == BrachaTouegDeadlockMessage.GRANT) {
          handleGrantMessage(vertex, message);
        } else if (type == BrachaTouegDeadlockMessage.DONE ||
            type == BrachaTouegDeadlockMessage.ACK) {
          /* Both ACK and DONE Messages are handled in the same way. The
             action take afterwards is independent on these types of
             messages.  */
          value.receivedMessage(message.getSenderId(), message.getType());
        }
      }

      ackSenderId = value.getIdWithInHoldAck();
      if (value.isFree() &&
          !value.isWaitingForMessage(BrachaTouegDeadlockMessage.ACK) &&
          !ackSenderId.equals(BrachaTouegDeadlockVertexValue.INVALID_ID)) {

        sendAckMessage(ackSenderId, vertex);
        value.setIdWithInHoldAck(BrachaTouegDeadlockVertexValue.INVALID_ID);
      }

      /* if all the ACK and DONE messages have been received, the vertex can
         send the pending DONE message to the parent and vote to halt */
      if (value.isNotified() &&
          !value.isWaitingForMessage(BrachaTouegDeadlockMessage.ACK) &&
          !value.isWaitingForMessage(BrachaTouegDeadlockMessage.DONE)) {

        Long senderId = value.getIdWithInHoldDone();

        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex ID " + vertex.getId() +
              " sent the last DONE message.");
          LOG.debug("Vertex ID " + vertex.getId() + " voted to halt.");
        }

        /* the initiator vertex does not need to send the DONE message since
           it is the starting point of the algorithm */
        if (!isInitiator(vertex) &&
            !senderId.equals(BrachaTouegDeadlockVertexValue.INVALID_ID)) {
          sendMessage(vertex.getId().get(), senderId,
              BrachaTouegDeadlockMessage.DONE);
          value.setIdWithInHoldDone(BrachaTouegDeadlockVertexValue.INVALID_ID);
        }

        vertex.voteToHalt();
      }
    }
  }

  /**
   * check whether the vertex is the initiator of the algorithm
   *
   * @param vertex Vertex
   * @return True if the vertex is the initiator
   */
  private boolean isInitiator(Vertex<LongWritable, ?, ?> vertex) {
    return vertex.getId().get() == BRACHA_TOUEG_DL_INITIATOR_ID.get(getConf());
  }

  /**
   * Initializes the algorithm by sending the control message for ID exchange
   * and preparing the value of the vertex.
   *
   * @param  vertex  vertex from which the control message is sent
   */
  private void initAlgorithm(Vertex<LongWritable,
      BrachaTouegDeadlockVertexValue, LongWritable> vertex) {

    BrachaTouegDeadlockVertexValue value;
    HashMap<Long, ArrayList<Long>> requests =
        new HashMap<Long, ArrayList<Long>>();
    long vertexId = vertex.getId().get();

    /* prepare the pending requests tracking data structure */
    for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
      ArrayList<Long> targets;
      Long tag = Long.valueOf(edge.getValue().get());
      Long target = Long.valueOf(edge.getTargetVertexId().get());

      if (requests.containsKey(tag)) {
        targets = requests.get(tag);
      } else {
        targets = new ArrayList<Long>();
      }

      targets.add(target);
      requests.put(tag, targets);
    }

    /* save in the value the number of requests that the node needs to get
       satisfied to consider itself free */
    value = new BrachaTouegDeadlockVertexValue(requests);
    vertex.setValue(value);

    /* send to all the outgoint edges the id of the current vertex */
    for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
      sendMessage(vertexId, edge.getTargetVertexId().get(),
          BrachaTouegDeadlockMessage.CTRL_IN_EDGE);
    }
  }

  /**
   * Send message wrapper for the Bracha Toueg algorithm specific for ACK
   * messages.
   *
   * @param receiver      recipient of the message
   * @param vertex        vertex sending the message
   */
  private void sendAckMessage(long receiver, Vertex<LongWritable,
      BrachaTouegDeadlockVertexValue, LongWritable> vertex) {

    this.sendMessage(Long.valueOf(vertex.getId().get()),
        receiver, BrachaTouegDeadlockMessage.ACK);

    if (!vertex.getValue().isNotified()) {
      vertex.voteToHalt();
    }
  }

  /**
   * Send message wrapper for the Bracha Toueg algorithm
   *
   * @param sender        sender of the message
   * @param receiver      recipient of the message
   * @param messageType   type of message to be sent
   */
  private void sendMessage(long sender, long receiver, long messageType) {
    BrachaTouegDeadlockMessage  message;

    message = new BrachaTouegDeadlockMessage(sender, messageType);
    sendMessage(new LongWritable(receiver), message);
    if (LOG.isDebugEnabled()) {
      LOG.debug("sent message " + message + " from " + sender +
          " to " + receiver);
    }
  }

  /**
   * this is a debugging function to verify that all parents have been
   * detected.
   *
   * @param vertex    vertex which collected its parents
   */
  private void logParents(Vertex<LongWritable,
      BrachaTouegDeadlockVertexValue,
      LongWritable> vertex) {
    ArrayList<Long> parents = vertex.getValue().getParents();
    int sz = parents.size();
    StringBuffer buffer = new StringBuffer();

    buffer.append("Vertex " + vertex.getId() + " parents:");
    for (int i = 0; i < sz; ++i) {
      buffer.append(" - " + parents.get(i));
    }
    LOG.debug(buffer.toString());
  }

  /**
   * This method resembles the notify_u procedure of the Bracha-Toueg algorithm.
   * It proceeds by sending a NOTIFY message via its outgoing edges and waits
   * for a DONE message from each destination node. If no pending requests need
   * to be awaited, the grant_u procedure is called. The latter case is
   * encounterd when the "wave" of NOTIFY messages reaches the edge of the
   * graph.
   *
   * @param vertex  the vertex on which the notify method i called
   */
  private void notifyVertices(
      Vertex<LongWritable, BrachaTouegDeadlockVertexValue, LongWritable> vertex) {

    BrachaTouegDeadlockVertexValue value = vertex.getValue();
    long vertexId = vertex.getId().get();
    boolean hasOutEdges = false;

    value.setNotified();

    for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
      hasOutEdges = true;
      sendMessage(vertexId,
          edge.getTargetVertexId().get(),
          BrachaTouegDeadlockMessage.NOTIFY);

      /* the node will wait for a DONE message from each notified vertex */
      value.waitForMessage(Long.valueOf(edge.getTargetVertexId().get()),
          Long.valueOf(BrachaTouegDeadlockMessage.DONE));
    }

    /* if no requests are pending, the node has to start GRANTing to all
       incoming edges */
    if (!hasOutEdges && isInitiator(vertex)) {
      value.setFree();
    } else if (!value.hasPendingRequests() && !value.isFree()) {
      grantVertices(vertex);
    }
  }

  /**
   * @param vertex      vertex on which the grant method is called
   */
  private void grantVertices(
      Vertex<LongWritable, BrachaTouegDeadlockVertexValue, LongWritable> vertex) {

    BrachaTouegDeadlockVertexValue value = vertex.getValue();
    ArrayList<Long> parents = value.getParents();
    long vertexId = vertex.getId().get();

    value.setFree();

    /* grant all the parents with resource access */
    for (Long parent : parents) {
      sendMessage(vertexId, parent,
          BrachaTouegDeadlockMessage.GRANT);

      /* the node will wait for a ACK message for each GRANTed vertex */
      value.waitForMessage(parent,
          Long.valueOf(BrachaTouegDeadlockMessage.ACK));
    }
  }

  /**
   * Function to handle the cases when a NOTIFY message is received.
   * If the message received is of type NOTIFY we distinguish two cases:
   * 1. The node was not yet notified: in this case  it forwards the
   *    NOTIFY message to its outgoing messages. In this phase the
   *    {@link BrachaTouegDeadlockComputation#notifyVertices} function is
   *    called.
   *    NB: in this case there is the need to keep track of the sender
   *        of the message since later a DONE must be sent back.
   * 2. The node was notified: in this case the node will immediately
   *    reply with a DONE message.
   *
   * @param vertex    vertex that received the DONE message
   * @param message   message received by the vertex
   */
  private void handleNotifyMessage(
      Vertex<LongWritable, BrachaTouegDeadlockVertexValue, LongWritable> vertex,
      BrachaTouegDeadlockMessage message) {

    BrachaTouegDeadlockVertexValue value = vertex.getValue();

    if (!value.isNotified()) {
      notifyVertices(vertex);
      value.setIdWithInHoldDone(message.getSenderId());
    } else {
      sendMessage(vertex.getId().get(), message.getSenderId(),
          BrachaTouegDeadlockMessage.DONE);
    }
  }

  /**
   * Function to handle the cases when a GRANT message is received.
   * When a GRANT message is received the number of requests is
   * decremented. In this case we must distinguish three cases:
   * 1. The number of requests needed reaches zero: at this stage a
   *    round of {@link BrachaTouegDeadlockComputation#grantVertices} is
   *    started to forward the resource granting mechanism.
   *    NB: the sender id of the node must be kept to handle the delivery of
   *    the ACK to the sender at the end of the granting procedure.
   * 2. The node already started go grant since it is free: in this case an ACK
   *    message is immediately sent back to the sender.
   * 3. The number of requests is bigger than zero: in this case an ACK
   *    is sent back to the sender.
   *
   * @param vertex    vertex that received the ACK message
   * @param message   message received by the vertex
   */
  private void handleGrantMessage(
      Vertex<LongWritable, BrachaTouegDeadlockVertexValue, LongWritable> vertex,
      BrachaTouegDeadlockMessage message) {

    BrachaTouegDeadlockVertexValue value = vertex.getValue();
    Long senderId = Long.valueOf(message.getSenderId());
    LongWritable wId = new LongWritable(senderId);
    LongWritable tag = vertex.getEdgeValue(wId);

    value.removeRequest(tag, wId);

    if (value.isFree() || value.getNumOfRequests(tag) > 0) {
      sendAckMessage(senderId, vertex);
      return;
    } else {
      grantVertices(vertex);
      value.setIdWithInHoldAck(senderId);
    }
  }
}
