/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.parallel.mm;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public interface GiraphMessageManager<
        OID_T extends WritableComparable,
        VDATA_T extends Writable,
        EDATA_T extends Writable,
        IN_MSG_T extends Writable,
        OUT_MSG_T extends Writable,
        GS_VID_T,
        GS_OID_T> {

    /**
     * Called by our framework, to deserialize the messages from c++ to java. Must be called before
     * getMessages
     */
    void receiveMessages();

    /**
     * Get the messages received from last round.
     *
     * @param lid local id.
     * @return received msg.
     */
    Iterable<IN_MSG_T> getMessages(long lid);

    /**
     * Check any message available on this vertex.
     *
     * @param lid local id
     * @return true if recevied messages.
     */
    boolean messageAvailable(long lid);

    /**
     * Send one message to dstOid.
     *
     * @param dstOid  vertex to receive this message.
     * @param message message.
     */
    void sendMessage(OID_T dstOid, OUT_MSG_T message);

    /**
     * Send msg to all neighbors of vertex.
     *
     * @param vertex  querying vertex
     * @param message message to send.
     */
    void sendMessageToAllEdges(Vertex<OID_T, VDATA_T, EDATA_T> vertex, OUT_MSG_T message);

    /**
     * Make sure all messages has been sent.
     */
    void finishMessageSending();

    /**
     * Check any messages received. For mpi-based message manager, we check any message to self
     * produced. For netty-based message manager, we check any message received.
     *
     * @return true if any message received.
     */
    boolean anyMessageReceived();

    void forceContinue();

    void preSuperstep();

    void postSuperstep();

    void postApplication();
}
