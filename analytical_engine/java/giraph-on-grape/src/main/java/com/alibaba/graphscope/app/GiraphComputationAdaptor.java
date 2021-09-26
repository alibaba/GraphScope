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
package com.alibaba.graphscope.app;

import com.alibaba.graphscope.communication.Communicator;
import com.alibaba.graphscope.context.DefaultContextBase;
import com.alibaba.graphscope.context.GiraphComputationAdaptorContext;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.mm.GiraphMessageManager;
import com.alibaba.graphscope.parallel.mm.MessageIterable;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.AggregatorManager;
import org.apache.giraph.graph.VertexDataManager;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This adaptor bridges c++ driver app and Giraph Computation.
 *
 * <p>Using raw types since we are not aware of Computation type parameters at this time.
 *
 * @param <OID_T> grape oid.
 * @param <VID_T> grape vid.
 * @param <VDATA_T> grape vdata.
 * @param <EDATA_T> grape edata.
 */
public class GiraphComputationAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> extends Communicator
        implements DefaultAppBase<
                OID_T,
                VID_T,
                VDATA_T,
                EDATA_T,
                GiraphComputationAdaptorContext<OID_T, VID_T, VDATA_T, EDATA_T>> {

    private static Logger logger = LoggerFactory.getLogger(GiraphComputationAdaptor.class);

    /**
     * Partial Evaluation to implement.
     *
     * @param graph fragment. The graph fragment providing accesses to graph data.
     * @param context context. User defined context which manages data during the whole
     *     computations.
     * @param messageManager The message manger which manages messages between fragments.
     * @see IFragment
     * @see DefaultContextBase
     * @see DefaultMessageManager
     */
    @Override
    public void PEval(
            IFragment<OID_T, VID_T, VDATA_T, EDATA_T> graph,
            DefaultContextBase<OID_T, VID_T, VDATA_T, EDATA_T> context,
            DefaultMessageManager messageManager) {

        GiraphComputationAdaptorContext ctx = (GiraphComputationAdaptorContext) context;
        /**
         * In c++ PEVal, we initialized this class' parent class: Communicator, now we set to
         * aggregator manager.
         */
        ctx.getAggregatorManager().init(getFFICommunicator());

        AbstractComputation userComputation = ctx.getUserComputation();
        GiraphMessageManager giraphMessageManager = ctx.getGiraphMessageManager();
        WorkerContext workerContext = ctx.getWorkerContext();
        AggregatorManager aggregatorManager = ctx.getAggregatorManager();
        // Before computation, we execute preparation methods provided by user's worker context.
        try {
            workerContext.preApplication();
        } catch (Exception e) {
            logger.error("Exception in workerContext preApplication: " + e.getMessage());
            return;
        }

        workerContext.preSuperstep();
        /** Execute master compute before super step */
        if (ctx.hasMasterCompute()) {
            MasterCompute masterCompute = ctx.getMasterCompute();
            if (!masterCompute.isHalted()) {
                masterCompute.compute();
                masterCompute.incSuperStep();
            }
        }

        aggregatorManager.preSuperstep();
        giraphMessageManager.preSuperstep();
        // In first round, there is no message, we pass an empty iterable.
        //        Iterable<LongWritable> messages = new MessageIterable<>();
        Iterable<Writable> messages = MessageIterable.emptyMessageIterable;

        VertexDataManager vertexDataManager = ctx.vertex.getVertexDataManager();
        VertexIdManager vertexIdManager = ctx.vertex.getVertexIdManager();
        int cnt = 0;
        for (Vertex<VID_T> grapeVertex : graph.innerVertices().longIterable()) {
            if (cnt > 5) {
                break;
            }
            if (ctx.getUserComputation().getConf().getGrapeVidClass().equals(Long.class)) {
                Long lid = (Long) grapeVertex.GetValue();
                logger.info(
                        "Vertex: "
                                + grapeVertex.GetValue()
                                + ", oid: "
                                + vertexIdManager.getId(lid)
                                + ", vdata: "
                                + vertexDataManager.getVertexData(lid));
                Vertex<VID_T> oid_tVertex =
                        (Vertex<VID_T>) FFITypeFactoryhelper.newVertex(Long.class);
                Long longOid = ((LongWritable) vertexIdManager.getId(lid)).get();
                graph.getVertex((OID_T) longOid, oid_tVertex);
            } else if (ctx.getUserComputation()
                    .getConf()
                    .getGrapeVidClass()
                    .equals(Integer.class)) {
                Integer lid = (Integer) grapeVertex.GetValue();
                logger.info(
                        "Vertex: "
                                + grapeVertex.GetValue()
                                + ", oid: "
                                + vertexIdManager.getId(lid)
                                + ", vdata: "
                                + vertexDataManager.getVertexData(lid));
            }
            cnt += 1;
        }

        try {
            for (long lid = 0; lid < graph.getInnerVerticesNum(); ++lid) {
                ctx.vertex.setLocalId((int) lid);
                userComputation.compute(ctx.vertex, messages);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // PostStep should run before finish message sending
        workerContext.postSuperstep();

        // wait msg send finish.
        giraphMessageManager.finishMessageSending();
        //        ctx.getFFICommunicator().barrier();
        giraphMessageManager.postSuperstep();
        // Sync aggregators
        aggregatorManager.postSuperstep();

        // increase super step
        userComputation.incStep();
        workerContext.setCurStep(1);

        // We can not judge whether to proceed by messages sent and check halted array.
        logger.info(
                "Any msg received: {} all halted {}",
                giraphMessageManager.anyMessageReceived(),
                ctx.allHalted());
        if (giraphMessageManager.anyMessageReceived() || !ctx.allHalted()) {
            messageManager.ForceContinue();
        }

        // do aggregation.
    }

    /**
     * Incremental Evaluation to implement.
     *
     * @param graph fragment. The graph fragment providing accesses to graph data.
     * @param context context. User defined context which manages data during the whole
     *     computations.
     * @param messageManager The message manger which manages messages between fragments.
     * @see IFragment
     * @see DefaultContextBase
     * @see DefaultMessageManager
     */
    @Override
    public void IncEval(
            IFragment<OID_T, VID_T, VDATA_T, EDATA_T> graph,
            DefaultContextBase<OID_T, VID_T, VDATA_T, EDATA_T> context,
            DefaultMessageManager messageManager) {

        GiraphComputationAdaptorContext ctx = (GiraphComputationAdaptorContext) context;
        AbstractComputation userComputation = ctx.getUserComputation();
        GiraphMessageManager giraphMessageManager = ctx.getGiraphMessageManager();
        WorkerContext workerContext = ctx.getWorkerContext();
        AggregatorManager aggregatorManager = ctx.getAggregatorManager();
        // Worker context
        workerContext.preSuperstep();

        // 0. receive messages
        giraphMessageManager.receiveMessages();

        /** execute master compute on master node if there is */
        if (ctx.hasMasterCompute()) {
            MasterCompute masterCompute = ctx.getMasterCompute();
            if (!masterCompute.isHalted()) {
                masterCompute.compute();
                masterCompute.incSuperStep();
            }
        }
        // Clear non-persistent aggregator
        aggregatorManager.preSuperstep();
        giraphMessageManager.preSuperstep();

        // 1. compute
        try {
            for (long lid = 0; lid < graph.getInnerVerticesNum(); ++lid) {
                if (ctx.vertex.isHalted() && giraphMessageManager.messageAvailable(lid)) {
                    ctx.activateVertex(lid); // set halted[lid] to false;
                }
                if (!ctx.isHalted(lid)) {
                    ctx.vertex.setLocalId((int) lid);
                    userComputation.compute(ctx.vertex, giraphMessageManager.getMessages(lid));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        workerContext.postSuperstep();

        // 2. send msg
        giraphMessageManager.finishMessageSending();
        // Sync aggregators
        aggregatorManager.postSuperstep();
        giraphMessageManager.postSuperstep();

        // increase super step
        userComputation.incStep();
        // Also increase worker context.
        workerContext.incStep();
        logger.info(
                "Any msg received: {} all halted {}",
                giraphMessageManager.anyMessageReceived(),
                ctx.allHalted());

        if (giraphMessageManager.anyMessageReceived() || !ctx.allHalted()) {
            messageManager.ForceContinue();
        }
    }
}
