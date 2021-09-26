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

import org.apache.hadoop.io.Writable;

/**
 * Data sent via a message that includes the source vertex id.
 */
public class BrachaTouegDeadlockMessage implements Writable {
  /**
   * Bracha-Toueg NOTICE message. This message is sent by a vertex on all its
   * outgoing edges.
   */
  public static final long NOTIFY = 1;
  /**
   * Bracha-Toueg GRANT message. This message is sent by a vertex on all its
   * incoming edges.
   */
  public static final long GRANT = 1 << 1;
  /**
   * Bracha-Toueg ACK message. This message is sent by a vertex on its
   * outgoing edges.
   */
  public static final long ACK = 1 << 2;
  /**
   * Bracha-Toueg DONE message. This message is sent by a vertex on its
   * incoming edges.
   */
  public static final long DONE = 1 << 3;
  /** Bracha-Toueg control incoming-edge message */
  public static final long CTRL_IN_EDGE = 1 << 4;

  /** Vertex ID of the sender. */
  private long  senderId;
  /** Message type. */
  private long  type;

  /** Default empty constructor. */
  public BrachaTouegDeadlockMessage() { /* no action */ }

  /**
   * @param id        id of the vertex
   * @param type      actual message content
   */
  public BrachaTouegDeadlockMessage(long id, long type) {
    this.senderId = id;
    this.type = type;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    senderId = input.readLong();
    this.type = input.readLong();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeLong(senderId);
    output.writeLong(this.type);
  }

  /**
   * @return long the id
   */
  public long getSenderId() {
    return senderId;
  }

  /**
   * @return long the type
   */
  public long getType() {
    return type;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();

    buffer.append("Message ");
    buffer.append("{ sender: " +  this.senderId + "; type: ");
    if (this.type == BrachaTouegDeadlockMessage.NOTIFY) {
      buffer.append("notify");
    } else if (this.type == BrachaTouegDeadlockMessage.GRANT) {
      buffer.append("grant");
    } else if (this.type == BrachaTouegDeadlockMessage.ACK) {
      buffer.append("ack");
    } else if (this.type == BrachaTouegDeadlockMessage.DONE) {
      buffer.append("done");
    } else if (this.type == BrachaTouegDeadlockMessage.CTRL_IN_EDGE) {
      buffer.append("<ctrl>");
    } else {
      buffer.append("unknown");
    }
    buffer.append(" }");

    return buffer.toString();
  }
}
