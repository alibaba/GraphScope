package com.alibaba.graphscope.example.circle.parallel.formal.store.memory;

import com.alibaba.graphscope.example.circle.parallel.formal.CircleAppParallelContext;
import com.alibaba.graphscope.example.circle.parallel.formal.store.ComputeStep;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.carrotsearch.hppc.LongArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * 内存模式计算
 *
 * @author liumin
 * @date 2024-08-11
 */
public class MemoryComputeStepImpl extends ComputeStep {
    private static final Logger logger = LoggerFactory.getLogger(MemoryComputeStepImpl.class);

    public MemoryComputeStepImpl(CircleAppParallelContext ctx) {
        super(ctx);
    }

    /**
     * 初始化处理
     *
     * @param frag           子图
     * @param messageManager 消息管理器
     */
    @Override
    public void initial(IFragment<Long, Long, Long, Long> frag, ParallelMessageManager messageManager) {
        // 首次处理结束后，交换 currentPaths 和 nextPaths
        ctx.swapPaths();
    }

    /**
     * 迭代计算
     *
     * @param frag           子图
     * @param messageManager 消息管理器
     */
    @Override
    public void inc(IFragment<Long, Long, Long, Long> frag, ParallelMessageManager messageManager) {
        // 接收 消息
        // outer vertex
        receiveMessage(frag, messageManager);

        if (ctx.superStep < ctx.maxIteration) {
            // 发送消息
            sendMessageThroughOE(frag, messageManager, 0, frag.getInnerVerticesNum(), 0);

            ctx.swapPaths();
        } else {
            ctx.currentPaths.clear();
        }
    }

    /**
     * 发送消息后处理步骤
     * 本机发送给 frag 内部的 inner vertex
     *
     * @param graph 子图
     * @param nbrV  邻居节点
     * @param msg   消息
     */
    @Override
    public void sendToLocalFrag(IFragment<Long, Long, Long, Long> graph, Vertex<Long> nbrV, List<LongArrayList> msg) {
        // Update local nbr vertex
        long neiOid = graph.getId(nbrV);
        for (int i = 0; i < msg.size(); i++) {
            LongArrayList clonePath = msg.get(i).clone();
            // 将 点 添加到路径
            clonePath.add(neiOid);
            msg.set(i, clonePath);
        }
        ctx.addPathToNextPathsAndFindCircle(nbrV.getValue().intValue(), msg);

        if (graph.getInnerVerticesNum() < 30) {
            logger.info("superStep is {},id is {},size is {},next path is {}", ctx.superStep, neiOid, msg.size(), ctx.nextPaths.get(nbrV.getValue().intValue()));
        }
    }

    /**
     * 接收消息后处理步骤
     *
     * @param frag 子图
     * @param lid  内部自增id
     * @param oid  原始id
     * @param msgs 消息列表
     */
    @Override
    public void processUponReceiveMsg(IFragment<Long, Long, Long, Long> frag, int lid, long oid, List<LongArrayList> msgs) {
        vprog(lid, oid, msgs);
        if (frag.getInnerVerticesNum() < 30) {
            logger.info("vprog  end.superStep is {},lid is {},oid is {},vdata is {},msg is {},currentPaths is {}", ctx.superStep, lid, oid, ctx.currentPaths.get(lid), msgs, ctx.currentPaths);
        }
    }

    @Override
    public Set<LongArrayList> getVertexPath(int lid, Integer batchId) {
        return ctx.getCurrentPaths(lid);
    }

    /**
     * 更新点属性，将符合条件的点 id 添加到路径集合
     * frag 内部的点在当前 superStep 中更新属性
     * frag 外部的点在下一 superStep 中接收消息并更新属性
     *
     * @param lid  当前点lid
     * @param oid  点原始id
     * @param msgs 发送给该点的消息。需保证发过来的消息提前过滤
     */
    private void vprog(int lid, long oid, List<LongArrayList> msgs) {
        // 消息处理
        for (LongArrayList path : msgs) {
            // 将 点 添加到路径
            path.add(oid);
        }

        // 将单次迭代 消息更新到 current集合
        ctx.addPathToCurrentPathsAndFindCircle(lid, msgs);
    }

}
