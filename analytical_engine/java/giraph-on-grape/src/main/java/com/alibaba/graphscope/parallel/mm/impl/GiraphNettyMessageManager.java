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
package com.alibaba.graphscope.parallel.mm.impl;

import static org.apache.giraph.conf.GiraphConstants.MAX_CONN_TRY_ATTEMPTS;
import static org.apache.giraph.conf.GiraphConstants.MAX_IPC_PORT_BIND_ATTEMPTS;

import com.alibaba.graphscope.communication.FFICommunicator;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.impl.VertexImpl;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.cache.SendMessageCache;
import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.netty.NettyClient;
import com.alibaba.graphscope.parallel.netty.NettyServer;
import com.alibaba.graphscope.parallel.utils.NetworkMap;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Giraph message manager relies on netty for ipc communication.
 *
 * <p>One netty base message manager has a netty client and a netty server. Netty Client connect to
 * all other netty server, and netty server is connected by other netty clients.
 *
 * <p>Has similar role with WorkerClientRequestProcessor in Giraph.
 *
 * @param <OID_T>     original id
 * @param <VDATA_T>   vertex data
 * @param <EDATA_T>   edge data
 * @param <IN_MSG_T>  incoming msg type
 * @param <OUT_MSG_T> outgoing msg type
 */
public class GiraphNettyMessageManager<
                OID_T extends WritableComparable,
                VDATA_T extends Writable,
                EDATA_T extends Writable,
                IN_MSG_T extends Writable,
                OUT_MSG_T extends Writable,
                GS_VID_T,
                GS_OID_T>
        extends AbstractMessageManager<
                OID_T, VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T, GS_VID_T, GS_OID_T> {

    private static Logger logger = LoggerFactory.getLogger(GiraphNettyMessageManager.class);

    private NetworkMap networkMap;

    private SendMessageCache<OID_T, OUT_MSG_T, GS_VID_T> outMessageCache;
    private NettyClient client;
    private NettyServer<OID_T, GS_VID_T> server;

    /**
     * The constructor is the preApplication.
     *
     * @param fragment   fragment to use
     * @param networkMap network map
     * @param conf       configuration
     */
    public GiraphNettyMessageManager(
            IFragment fragment,
            NetworkMap networkMap,
            DefaultMessageManager mm,
            ImmutableClassesGiraphConfiguration<OID_T, VDATA_T, EDATA_T> conf,
            FFICommunicator communicator) {
        super(fragment, mm, conf, communicator);
        this.networkMap = networkMap;
        // Netty server depends on message store.
        initNetty();

        // Create different type of message cache as needed.
        outMessageCache =
                (SendMessageCache<OID_T, OUT_MSG_T, GS_VID_T>)
                        SendMessageCache.newMessageCache(fragNum, fragId, client, conf);
    }

    public void initNetty() {
        logger.info(
                "Creating server on "
                        + networkMap.getSelfWorkerId()
                        + " max bind time: "
                        + MAX_IPC_PORT_BIND_ATTEMPTS.get(getConf()));
        server =
                new NettyServer(
                        getConf(),
                        fragment,
                        networkMap,
                        nextIncomingMessageStore,
                        (Thread t, Throwable e) -> logger.error(t.getId() + ": " + e.toString()));
        server.startServer();
        // No barrier
        //        getCommunicator().barrier();

        logger.info(
                "Create client on "
                        + networkMap.getSelfWorkerId()
                        + " max times: "
                        + MAX_CONN_TRY_ATTEMPTS.get(getConf()));
        client =
                new NettyClient(
                        getConf(),
                        networkMap,
                        (Thread t, Throwable e) -> logger.error(t.getId() + ": " + e.toString()));
        client.connectToAllAddress();
        logger.info(
                "Worker ["
                        + networkMap.getSelfWorkerId()
                        + "] listen on "
                        + networkMap.getAddress()
                        + ", client: "
                        + client.toString());
    }

    /**
     * Called by our framework, to deserialize the messages from c++ to java. Must be called before
     * getMessages
     */
    @Override
    public void receiveMessages() {
        // No op
        logger.debug(
                "Messager [{}] receive totally {} bytes", fragId, server.getNumberOfByteReceived());
        server.resetBytesCounter();
    }

    /**
     * Send one message to dstOid.
     *
     * @param dstOid  vertex to receive this message.
     * @param message message.
     */
    @Override
    public void sendMessage(OID_T dstOid, OUT_MSG_T message) {
        if (dstOid instanceof LongWritable) {
            Long longOid = ((LongWritable) dstOid).get();
            if (!fragment.getVertex((GS_OID_T) longOid, grapeVertex)) {
                throw new IllegalStateException("get lid failed for oid: " + longOid);
            }
            sendLidMessage(grapeVertex, message);
        } else {
            throw new IllegalStateException("Expect a long writable");
        }
    }

    /**
     * Send msg to all neighbors of vertex.
     *
     * @param vertex  querying vertex
     * @param message message to send.
     */
    @Override
    public void sendMessageToAllEdges(Vertex<OID_T, VDATA_T, EDATA_T> vertex, OUT_MSG_T message) {
        VertexImpl<GS_VID_T, OID_T, VDATA_T, EDATA_T> vertexImpl =
                (VertexImpl<GS_VID_T, OID_T, VDATA_T, EDATA_T>) vertex;
        grapeVertex.SetValue((GS_VID_T) (Long) vertexImpl.getLocalId());

        // send msg through outgoing adjlist
        for (Nbr<GS_VID_T, ?> nbr : fragment.getOutgoingAdjList(grapeVertex).iterable()) {
            com.alibaba.graphscope.ds.Vertex<GS_VID_T> curVertex = nbr.neighbor();
            sendLidMessage(curVertex, message);
        }
    }

    private void sendLidMessage(
            com.alibaba.graphscope.ds.Vertex<GS_VID_T> nbrVertex, OUT_MSG_T message) {
        int dstfragId = fragment.getFragId(nbrVertex);
        outMessageCache.sendMessage(dstfragId, fragment.vertex2Gid(nbrVertex), message);
    }

    /**
     * Make sure all messages has been sent.
     */
    @Override
    public void finishMessageSending() {
        outMessageCache.flushMessage();
        /** Add to self cache, IN_MSG_T must be same as OUT_MSG_T */
        outMessageCache.removeMessageToSelf(
                (MessageStore<OID_T, OUT_MSG_T, GS_VID_T>) nextIncomingMessageStore);
    }

    @Override
    public void preSuperstep() {
        server.preSuperStep((MessageStore<OID_T, Writable, GS_VID_T>) nextIncomingMessageStore);
    }

    @Override
    public void postSuperstep() {
        // First wait all message arrived.
        client.postSuperStep();
        outMessageCache.clear();
        currentIncomingMessageStore.swap(nextIncomingMessageStore);
        nextIncomingMessageStore.clearAll();
    }

    @Override
    public void postApplication() {
        logger.info("Closing Client...");
        client.close();
        logger.info("Closing Server...");
        server.close();
    }
}
