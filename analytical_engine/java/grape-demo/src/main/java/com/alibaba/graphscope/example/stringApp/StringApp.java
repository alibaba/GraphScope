package com.alibaba.graphscope.example.stringApp;

import com.alibaba.graphscope.app.ParallelAppBase;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.ds.StringView;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelEngine;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringApp
        implements ParallelAppBase<Long, Long, StringView, StringView, StringAppContext>,
                ParallelEngine {

    private static Logger logger = LoggerFactory.getLogger(ParallelAppBase.class);

    /**
     * Partial Evaluation to implement.
     *
     * @param graph          fragment. The graph fragment providing accesses to graph data.
     * @param context        context. User defined context which manages data during the whole
     *                       computations.
     * @param messageManager The message manger which manages messages between fragments.
     * @see IFragment
     * @see ParallelContextBase
     * @see ParallelMessageManager
     */
    @Override
    public void PEval(
            IFragment<Long, Long, StringView, StringView> graph,
            ParallelContextBase<Long, Long, StringView, StringView> context,
            ParallelMessageManager messageManager) {

        StringAppContext ctx = (StringAppContext) context;

        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        long batch = graph.getInnerVerticesNum() / 10;
        for (long i = 0; i < graph.getInnerVerticesNum(); i += batch) {
            vertex.setValue(i);
            for (Nbr<Long, StringView> nbr : graph.getOutgoingAdjList(vertex).iterable()) {
                logger.info(
                        "Edge {}({})->{}({}), ed {}",
                        graph.getId(vertex),
                        graph.getData(vertex).toJavaString(),
                        graph.getId(nbr.neighbor()),
                        graph.getData(nbr.neighbor()),
                        nbr.data());
            }
        }

        messageManager.forceContinue();
    }

    /**
     * Incremental Evaluation to implement.
     *
     * @param graph          fragment. The graph fragment providing accesses to graph data.
     * @param context        context. User defined context which manages data during the whole
     *                       computations.
     * @param messageManager The message manger which manages messages between fragments.
     * @see IFragment
     * @see ParallelContextBase
     * @see ParallelMessageManager
     */
    @Override
    public void IncEval(
            IFragment<Long, Long, StringView, StringView> graph,
            ParallelContextBase<Long, Long, StringView, StringView> context,
            ParallelMessageManager messageManager) {}
}
