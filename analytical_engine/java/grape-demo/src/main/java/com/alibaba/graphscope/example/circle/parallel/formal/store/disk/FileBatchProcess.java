package com.alibaba.graphscope.example.circle.parallel.formal.store.disk;

import com.alibaba.graphscope.example.circle.parallel.formal.CircleAppParallelContext;
import com.alibaba.graphscope.example.circle.parallel.formal.CircleUtil;
import com.alibaba.graphscope.example.circle.parallel.formal.store.ComputeStep;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.carrotsearch.hppc.LongArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * 磁盘模式批次处理实现类
 * 批次读写文件，处理数据
 *
 * @author liumin
 * @date 2024-08-14
 */
public class FileBatchProcess implements IBatchProcess {
    private static final Logger logger = LoggerFactory.getLogger(FileBatchProcess.class);

    /**
     * 分批次读取 Message Store 更新 Path Storage 中的数据
     *
     * @param ctx            app context
     * @param graph          子图
     * @param messageStorage 消息管理器
     * @param pathStorage    待更新的pathStorage
     */
    @Override
    public void batchUpdatePathStorage(CircleAppParallelContext ctx, IFragment<Long, Long, Long, Long> graph, MessageStorage messageStorage, PathStorage pathStorage) {
        if (messageStorage.getBatchMessages().isEmpty()) {
            return;
        }
        long start = System.currentTimeMillis();

        // 将消息集合中发送给同一个点的 消息 聚合
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        for (Map.Entry<Integer, Set<LongArrayList>> integerListEntry : messageStorage.getBatchMessages().entrySet()) {
            Set<LongArrayList> receivedMessages = integerListEntry.getValue();
            int toLid = integerListEntry.getKey();
            if (receivedMessages.isEmpty()) {
                continue;
            }

            vertex.setValue((long) toLid);
            long oid = graph.getId(vertex);
            for (LongArrayList path : receivedMessages) {
                path.add(oid);
                if (CircleUtil.isCircle(path) && path.size() > ctx.minIteration) {
                    ctx.addPathToCircleResult(toLid, path);
                    continue;
                }

                pathStorage.putToStorage(toLid, path);
            }
        }

        logger.info("Inner batchUpdatePathStorage end.superStep is {},pathStoreFile is {},msg size is {},time is {}s", ctx.superStep, pathStorage.getPath(), pathStorage.getFragmentPaths().values().stream().mapToInt(Set::size).sum(), (System.currentTimeMillis() - start) / 1000);

        if (ctx.superStep < ctx.maxIteration) {
            if (!pathStorage.getFragmentPaths().isEmpty()) {
                pathStorage.dump();
            }
        } else {
            pathStorage.clearInMemory();
        }
        messageStorage.clearInMemory();
    }

    /**
     * 分批次发送消息
     *
     * @param ctx            app context
     * @param computeStep    计算步骤
     * @param graph          子图
     * @param ps             从 pathStore 中读取数据
     * @param messageManager 消息管理器
     */
    @Override
    public void sendMsg(CircleAppParallelContext ctx, ComputeStep computeStep, IFragment<Long, Long, Long, Long> graph, PathStorage ps, ParallelMessageManager messageManager) {
        long start = System.currentTimeMillis();
        computeStep.sendMessageThroughOE(graph, messageManager, ps.getBeginVertex(), ps.getEndVertex(), ps.getBatchId());

        // dump local nbr received msg to messageStorages
        ctx.diskStoreContext.messageStorages.forEach(FileObjectStorage::dump);
        logger.info("Inner batch sendMsg from file end.superStep is {},pathStoreFile is {},msg size is {},time is {}s", ctx.superStep, ps.getPath(), ps.getFragmentPaths().values().stream().mapToInt(Set::size).sum(), (System.currentTimeMillis() - start) / 1000);

        // clear memory after all messages are sent
        ps.clearInMemory();
    }

}
