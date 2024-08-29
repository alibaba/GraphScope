package com.alibaba.graphscope.example.circle.parallel.formal.store.disk;

import com.alibaba.graphscope.example.circle.parallel.formal.CircleAppParallelContext;
import com.alibaba.graphscope.example.circle.parallel.formal.store.ComputeStep;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;

/**
 * 磁盘模式批次处理接口
 *
 * @author liumin
 * @date 2024-08-14
 */
public interface IBatchProcess {
    /**
     * 分批次读取 Message Store 更新 Path Storage 中的数据
     *
     * @param ctx            app context
     * @param graph          子图
     * @param messageStorage 消息管理器
     * @param pathStorage    待更新的pathStorage
     */
    void batchUpdatePathStorage(CircleAppParallelContext ctx, IFragment<Long, Long, Long, Long> graph, MessageStorage messageStorage, PathStorage pathStorage);

    /**
     * 分批次发送消息
     *
     * @param ctx            app context
     * @param computeStep    计算步骤
     * @param graph          子图
     * @param ps             从 pathStore 中读取数据
     * @param messageManager 消息管理器
     */
    void sendMsg(CircleAppParallelContext ctx, ComputeStep computeStep, IFragment<Long, Long, Long, Long> graph, PathStorage ps, ParallelMessageManager
            messageManager);
}
