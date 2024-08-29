package com.alibaba.graphscope.example.circle.parallel.formal;

import com.alibaba.graphscope.app.ParallelAppBase;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * <ol>
 *     <li>lid:fragment内部从0开始编码,0-innerVerticesNum-1</li>
 *     <li>gid:前n位为frag_id, 中间是label_id, 后面是lid</li>
 *     <li>oid:外部输入文件的vertices id</li>
 * </ol>
 *
 * @author liumin
 * @date 2024-07-12
 */
public class CircleAppParallel implements ParallelAppBase<Long, Long, Long, Long, CircleAppParallelContext> {
    private static final Logger logger = LoggerFactory.getLogger(CircleAppParallel.class);

    /**
     * 1个 num worker 包含 1个fragment，该方法只会调用一次
     *
     * @param fragment           @param graph fragment. The graph fragment providing accesses to graph data. each fragment is a local object (i.e.,payloads live entirely on a local node).
     * @param defaultContextBase context. User defined context which manages data during the whole
     *                           computations.
     * @param messageManager     The message manger which manages messages between fragments.
     * @see IFragment
     * @see ParallelContextBase
     * @see ParallelMessageManager
     */
    @Override
    public void PEval(IFragment<Long, Long, Long, Long> fragment, ParallelContextBase<Long, Long, Long, Long> defaultContextBase, ParallelMessageManager messageManager) {
        CircleAppParallelContext ctx = (CircleAppParallelContext) defaultContextBase;
        logger.info("PEval start.show frag info,inner vertices num is {},outer vertices num is {},frag vertices num is {},graph vertices num is {},active vertices num is {}", fragment.getInnerVerticesNum(), fragment.getOuterVerticesNum(), fragment.getVerticesNum(), fragment.getTotalVerticesNum(), ctx.currentPaths.size());

        // 获取所有的内部点
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        for (long i = 0; i < fragment.getInnerVerticesNum(); ++i) {
            // 表示获取 到 frag 内部的第 几个 点
            vertex.setValue(i);
            try {
                // 内存处理
                ctx.storeProcessor.sendToNbr(fragment, messageManager, vertex, 0, 0);
            } catch (IOException e) {
                logger.error("PEval error", e);
            }
        }

        ctx.storeProcessor.initial(fragment, messageManager);

        ctx.currModified.assign(ctx.nextModified);
        int cardinality = ctx.nextModified.cardinality();
        if (cardinality > 0) {
            logger.info("PEval Continue,nextModified is {}", cardinality);
            messageManager.forceContinue();
        }

        ctx.removeTheadLocalVariable();
        // 不同 frag 有不同 超步
        ctx.superStep += 1;
    }

    /**
     * Inc compute, superStep start from 1
     *
     * @param frag
     * @param defaultContextBase
     * @param messageManager
     */
    @Override
    public void IncEval(IFragment<Long, Long, Long, Long> frag, ParallelContextBase<Long, Long, Long, Long> defaultContextBase, ParallelMessageManager messageManager) {
        CircleAppParallelContext ctx = (CircleAppParallelContext) defaultContextBase;
        ctx.nextModified.clearAll();
        // start from 1
        if (ctx.superStep > ctx.maxIteration) {
            logger.info("superStep exceeds maxIteration,superStep is {}", ctx.superStep);
            return;
        }

        ctx.storeProcessor.inc(frag, messageManager);

        logger.info("IncEval end.superStep is {},currModified cnt is {},local nextModified cnt is {}", ctx.superStep, ctx.currModified.cardinality(), ctx.nextModified.cardinality());
        // assign nextModified to currModified
        ctx.currModified.assign(ctx.nextModified);
        if (ctx.nextModified.cardinality() > 0) {
            messageManager.forceContinue();
        }

        ctx.superStep += 1;
    }
}
