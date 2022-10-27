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

import static org.apache.giraph.conf.GiraphConstants.MAX_OUT_MSG_CACHE_SIZE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_LONG;

import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime;
import com.alibaba.graphscope.communication.FFICommunicator;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.fragment.BaseArrowProjectedFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.adaptor.AbstractArrowProjectedAdaptor;
import com.alibaba.graphscope.graph.GiraphVertexIdManager;
import com.alibaba.graphscope.graph.impl.VertexImpl;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;
import com.alibaba.graphscope.utils.LongIdParser;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GiraphMpiMessageManager<
                OID_T extends WritableComparable,
                VDATA_T extends Writable,
                EDATA_T extends Writable,
                IN_MSG_T extends Writable,
                OUT_MSG_T extends Writable,
                GS_VID_T,
                GS_OID_T>
        extends AbstractMessageManager<
                OID_T, VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T, GS_VID_T, GS_OID_T> {

    public static long THRESHOLD;
    private static Logger logger = LoggerFactory.getLogger(GiraphMpiMessageManager.class);
    private FFIByteVectorOutputStream[] cacheOut;

    private LongIdParser idParser;
    private PropertyNbrUnit<Long> nbrUnit;
    private long offsetBeginPtrFirstAddr;
    private long offsetEndPtrFirstAddr;
    private long nbrUnitEleSize;
    private long nbrUnitInitAddress;
    private BaseArrowProjectedFragment<GS_OID_T, GS_VID_T, ?, ?> projectedFragment;
    private GiraphVertexIdManager<GS_VID_T, OID_T> idManager;

    public GiraphMpiMessageManager(
            IFragment fragment,
            DefaultMessageManager defaultMessageManager,
            ImmutableClassesGiraphConfiguration configuration,
            FFICommunicator communicator,
            GiraphVertexIdManager<GS_VID_T, OID_T> idManager) {
        super(fragment, defaultMessageManager, configuration, communicator);
        this.idManager = idManager;
        THRESHOLD = MAX_OUT_MSG_CACHE_SIZE.get(configuration);
        this.projectedFragment =
                ((AbstractArrowProjectedAdaptor<GS_OID_T, GS_VID_T, ?, ?>) fragment)
                        .getBaseArrayProjectedFragment();

        this.cacheOut = new FFIByteVectorOutputStream[fragment.fnum()];
        for (int i = 0; i < fragment.fnum(); ++i) {
            this.cacheOut[i] = new FFIByteVectorOutputStream();
            this.cacheOut[i].resize(THRESHOLD);
        }
        idParser = new LongIdParser(fragment.fnum(), 1);
        nbrUnit = (PropertyNbrUnit<Long>) this.projectedFragment.getOutEdgesPtr();
        offsetEndPtrFirstAddr = this.projectedFragment.getOEOffsetsEndPtr();
        offsetBeginPtrFirstAddr = this.projectedFragment.getOEOffsetsBeginPtr();
        nbrUnitEleSize = nbrUnit.elementSize();
        nbrUnitInitAddress = nbrUnit.getAddress();
    }

    /**
     * Called by our frame work, to deserialize the messages from c++ to java. Must be called before
     * getMessages
     */
    @Override
    public void receiveMessages() {
        // put message to currentIncoming message store
        FFIByteVector tmpVector = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
        long bytesOfReceivedMsg = 0;
        while (grapeMessager.getPureMessage(tmpVector)) {
            // The retrieved tmp vector has been resized, so the cached objAddress is not available.
            // trigger the refresh
            tmpVector.touch();
            // OutArchive will do the resize;
            if (logger.isDebugEnabled()) {
                logger.debug("Frag [{}] digest message of size {}", fragId, tmpVector.size());
            }
            ///////////////////////////////////////////
            currentIncomingMessageStore.digest(tmpVector);
            ///////////////////////////////////////////
            bytesOfReceivedMsg += tmpVector.size();
        }
        logger.info(
                "Frag [{}] totally Received [{}] bytes from others starting deserialization",
                fragId,
                bytesOfReceivedMsg);
    }

    /**
     * Send one message to dstOid.
     *
     * @param dstOid  vertex to receive this message.
     * @param message message.
     */
    @Override
    public void sendMessage(OID_T dstOid, OUT_MSG_T message) {
        GS_VID_T lid = idManager.oid2Lid(dstOid);
        grapeVertex.SetValue(lid);
        sendMessage(grapeVertex, message);
    }

    private void sendMessage(com.alibaba.graphscope.ds.Vertex<GS_VID_T> vertex, OUT_MSG_T msg) {
        int dstfragId = fragment.getFragId(vertex);
        if (cacheOut[dstfragId].bytesWriten() >= THRESHOLD && dstfragId != fragId) {
            cacheOut[dstfragId].writeLong(
                    0, cacheOut[dstfragId].bytesWriten() - 8); // minus size_of_long
            cacheOut[dstfragId].finishSetting();
            // the vertex will be swapped. so this vector is empty;
            grapeMessager.sendToFragment(dstfragId, cacheOut[dstfragId].getVector());
            //            cacheOut[dstfragId] = new FFIByteVectorOutputStream();
            //            cacheOut[dstfragId].resize(THRESHOLD);
            cacheOut[dstfragId].reset();
            cacheOut[dstfragId].writeLong(0, 0);
        }
        try {
            cacheOut[dstfragId].writeLong((Long) fragment.vertex2Gid(vertex));
            msg.write(cacheOut[dstfragId]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send message to neighbor vertices.
     *
     * @param vertex
     */
    @Override
    public void sendMessageToAllEdges(Vertex<OID_T, VDATA_T, EDATA_T> vertex, OUT_MSG_T message) {
        VertexImpl<GS_VID_T, OID_T, VDATA_T, EDATA_T> vertexImpl =
                (VertexImpl<GS_VID_T, OID_T, VDATA_T, EDATA_T>) vertex;

        long lid = vertexImpl.getLocalId();
        long offset = idParser.getOffset(lid);
        long oeBeginOffset = JavaRuntime.getLong(offsetBeginPtrFirstAddr + offset * 8);
        long oeEndOffset = JavaRuntime.getLong(offsetEndPtrFirstAddr + offset * 8);
        long curAddress = nbrUnitInitAddress + nbrUnitEleSize * oeBeginOffset;
        long endAddress = nbrUnitInitAddress + nbrUnitEleSize * oeEndOffset;

        while (curAddress < endAddress) {
            nbrUnit.setAddress(curAddress);
            grapeVertex.SetValue((GS_VID_T) nbrUnit.vid());
            sendMessage(grapeVertex, message);
            curAddress += nbrUnitEleSize;
        }

        // send msg through outgoing adjlist
        //        AdjList<GS_VID_T, ?> adaptorAdjList = fragment.getOutgoingAdjList(grapeVertex);
        //
        //        for (NbrBase<GS_VID_T, ?> nbr : adaptorAdjList.nbrBases()) {
        //            com.alibaba.graphscope.ds.Vertex<GS_VID_T> curVertex = nbr.neighbor();
        //            sendMessage(curVertex, message);
        //        }

    }

    /**
     * Make sure all messages has been sent. Clean outputstream buffer
     */
    @Override
    public void finishMessageSending() {
        for (int i = 0; i < fragNum; ++i) {
            long bytesWriten = cacheOut[i].bytesWriten();
            cacheOut[i].finishSetting();
            cacheOut[i].writeLong(0, bytesWriten - SIZE_OF_LONG);

            if (bytesWriten == SIZE_OF_LONG) {
                logger.debug(
                        "[Finish msg] sending skip msg from {} -> {}, since msg size: {}",
                        fragId,
                        i,
                        bytesWriten);
                continue;
            }
            if (i == fragId) {
                nextIncomingMessageStore.digest(cacheOut[i].getVector());
                logger.info(
                        "In final step, Frag [{}] digest msg to self of size: {}",
                        fragId,
                        bytesWriten);
            } else {
                grapeMessager.sendToFragment(i, cacheOut[i].getVector());
                logger.info(
                        "In final step, Frag [{}] send msg to [{}] of size: {}",
                        fragId,
                        i,
                        bytesWriten);
            }
        }
        //        if (maxSuperStep > 0) {
        //            grapeMessager.ForceContinue();
        //            maxSuperStep -= 1;
        //        }

        //        logger.debug("[Unused res] {}", unused);
        //        logger.debug("adaptor hasNext {}, grape hasNext{}", adaptorHasNext, grapeHasNext);
        //        logger.debug("adaptor next {}, grape next {}", adaptorNext, grapeNext);
        //        logger.debug("adaptor neighbor {}, grape neighbor {}", adaptorNeighbor,
        // grapeNeighbor);
    }

    @Override
    public void preSuperstep() {
        for (int i = 0; i < fragNum; ++i) {
            //            cacheOut[i].resize(THRESHOLD);
            cacheOut[i].reset();
            //            if (i != fragId) {
            // only write size info for mpi messages, local message don't need size.
            try {
                cacheOut[i].writeLong(0);
            } catch (IOException e) {
                e.printStackTrace();
            }
            //            }
        }
    }

    @Override
    public void postSuperstep() {
        currentIncomingMessageStore.swap(nextIncomingMessageStore);
        nextIncomingMessageStore.clearAll();
    }

    @Override
    public void postApplication() {}
}
