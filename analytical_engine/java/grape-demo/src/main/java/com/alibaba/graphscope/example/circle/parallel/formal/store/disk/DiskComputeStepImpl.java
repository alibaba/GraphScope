package com.alibaba.graphscope.example.circle.parallel.formal.store.disk;

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
import java.util.stream.Collectors;

/**
 * 环形磁盘模式计算
 *
 * @author liumin
 * @date 2024-08-11
 */
public class DiskComputeStepImpl extends ComputeStep {
    private static final Logger logger = LoggerFactory.getLogger(DiskComputeStepImpl.class);
    /**
     * disk mode 参数管理
     */
    private final DiskStoreContext diskStoreContext;
    /**
     * app context
     */
    private final CircleAppParallelContext ctx;
    /**
     * 批次处理类
     */
    private final IBatchProcess process;

    public DiskComputeStepImpl(CircleAppParallelContext ctx) {
        super(ctx);
        this.ctx = ctx;
        this.diskStoreContext = ctx.diskStoreContext;
        this.process = new FileBatchProcess();
    }

    /**
     * 初始化处理
     *
     * @param frag           子图
     * @param messageManager 消息管理器
     */
    @Override
    public void initial(IFragment<Long, Long, Long, Long> frag, ParallelMessageManager messageManager) {
        // 清理残留的磁盘文件
        diskStoreContext.clearDiskPathStorages();
        diskStoreContext.clearDiskMessageStorages();
        // 首次发送消息后，将未 flush 的 message 统一 flush
        diskStoreContext.messageStorages.forEach(FileObjectStorage::dump);
    }

    /**
     * 迭代计算
     *
     * @param frag           子图
     * @param messageManager 消息管理器
     */
    @Override
    public void inc(IFragment<Long, Long, Long, Long> frag, ParallelMessageManager messageManager) {
        // 1.接收消息并发写入 threadLocal MessageStore
        receiveMessage(frag, messageManager);

        // 2.从 MessageStore 按 batch 读取数据写入 PathStore
        for (int batchId = 0; batchId < diskStoreContext.fragBatchNum; ++batchId) {
            // 上一个超步发送过来的 msg 全部写入 messageStore 后，按 batchId 读取 messageStore 文件内容，合并消息
            // 更新完点属性，将该批次的数据增量 append 写入 pathStore 文件
            MessageStorage messageStorage = diskStoreContext.getMessageStorage(batchId);
            messageStorage.loadInBatchAndUpdateStorage(ctx, process, frag, batchId);
        }

        // 3.清除当前super step的消息
        diskStoreContext.clearMessageStorages();
        diskStoreContext.clearMemoryPathStorages();

        if (ctx.superStep < ctx.maxIteration) {
            // 4.从 PathStore 按 batch 读取数据发送消息
            for (int batchId = 0; batchId < diskStoreContext.fragBatchNum; ++batchId) {
                // 分 batch 读取 pathStore 文件，发送消息
                // 我们将在[0, batchNum - 1] 的super step中，分批次发送消息
                // frag 内部 inner vertex 之间发送的消息会被保存到 MessageStorage
                // frag 之间 outer vertex 之间发送的消息通过网络发送出去，会暂存到 C++ 的memory
                PathStorage pathStorage = diskStoreContext.getPathStorages(batchId);
                // load vertex attr from pathStore file and send message
                pathStorage.loadObjectsAndSendMessage(ctx, this, frag, process, messageManager);
            }
        }
        // 5. 清理当前super step的磁盘点属性文件
        diskStoreContext.clearDiskPathStorages();
    }

    /**
     * 发送消息后处理步骤
     * 本机发送给 frag 内部的 inner vertex，更新到内存
     *
     * <p> 磁盘模式下，消息可以切分成小批次处理。
     * 发送消息时，小批次的消息先暂存到内存，待达到阈值时，统一 flush 到 disk </p>
     * 注意: 该方法会在多线程环境下被调用
     *
     * @param frag 子图
     * @param nbrV 邻居节点
     * @param msg  消息
     */
    @Override
    public void sendToLocalFrag(IFragment<Long, Long, Long, Long> frag, Vertex<Long> nbrV, List<LongArrayList> msg) {
        int toLid = nbrV.getValue().intValue();
        int batchId = diskStoreContext.getBatchIdFromVertexId(toLid);
        MessageStorage messageStorage = diskStoreContext.getMessageStorage(batchId);
        // 将消息暂存到内存，后续统一 flush 到 disk
        msg.replaceAll(LongArrayList::clone);
        messageStorage.putToStorage(toLid, msg);
    }

    /**
     * 接收消息后处理步骤
     * 接收消息和发送消息的区别在于：接收的消息来源于网络传输，我们无法提前判断接收哪个点的消息以及一次接收多少消息
     * 所以，为了将消息切分成小批次，需要一边接收消息，一边判断消息数量是否到达阈值，达到阈值后需要flush到disk，最后清理内存
     * 注意：上述整个流程需要在多线程环境调用。要保证：1）更新内存数据结构和清理内存数据结构互相不影响 2）避免串行更新，有较好的性能。因此每个线程单独维护一个 messageStore，互不影响 {{{@link LocalThreadMessageStorage}}}
     *
     * @param frag 子图
     * @param lid  内部自增id
     * @param oid  原始id
     * @param msgs 消息列表
     */
    @Override
    public void processUponReceiveMsg(IFragment<Long, Long, Long, Long> frag, int lid, long oid, List<LongArrayList> msgs) {
        int batchId = diskStoreContext.getBatchIdFromVertexId(lid);
        LocalThreadMessageStorage messageStorage = diskStoreContext.getLocalMessageStorage(batchId);
        // local frag lid
        messageStorage.batchDumpMessages(lid, msgs);

        if (showLog(frag)) {
            messageStorage.getMessageStorage().load();
            logger.info("vprog  end.superStep is {},batchId is {},fid is {},lid is {},oid is {},msg is {},messageStore is {}", ctx.superStep, batchId, frag.fid(), lid, oid, msgs, diskStoreContext.threadLocalMessageStorageFactory.getAllValue().stream().map(i -> i.getMessageStorage().getBatchMessages().get(lid)).collect(Collectors.toList()));
        }
    }

    /**
     * 获取当前点的属性信息，即保留的路径列表
     * 磁盘模式下，superStep=0时，数据保留在内存
     *
     * @param lid     内部自增id
     * @param batchId frag 内部 batchId
     * @return 点的属性信息
     */
    @Override
    public Set<LongArrayList> getVertexPath(int lid, Integer batchId) {
        if (batchId == null) {
            batchId = diskStoreContext.getBatchIdFromVertexId(lid);
        }
        return ctx.superStep == 0 ? ctx.currentPaths.get(lid) : diskStoreContext.getPathStorages(batchId).getPath(lid);
    }
}
