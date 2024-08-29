package com.alibaba.graphscope.example.circle.parallel.formal.store.disk;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.fragment.IFragment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 磁盘模式 context
 *
 * @author liumin
 * @date 2024-08-12
 */
public class DiskStoreContext {
    private static final Logger logger = LoggerFactory.getLogger(DiskStoreContext.class);
    /**
     * 批次读写文件消息阈值
     * 仅 Disk 模式下
     */
    public final int msgThreshold;
    /**
     * 单个 frag 执行的batch数量
     */
    public int fragBatchNum;
    private final List<PathStorage> pathStorages;
    public List<MessageStorage> messageStorages;
    /**
     * frag 内部节点数
     */
    private final int ivnum;
    private final List<long[]> fragBatchRange;
    public LocalThreadMessageStorageFactory threadLocalMessageStorageFactory;

    /**
     * @param jsonObject input param from app
     */
    public DiskStoreContext(IFragment<Long, Long, Long, Long> frag, JSONObject jsonObject) {
        ivnum = (int) frag.getInnerVerticesNum();
        // frag batch
        if (!jsonObject.containsKey("fragBatchNum")) {
            fragBatchNum = 1;
        } else {
            fragBatchNum = jsonObject.getInteger("fragBatchNum");
        }

        if (fragBatchNum > ivnum) {
            fragBatchNum = ivnum;
        }
        // 单次处理消息的阈值
        if (!jsonObject.containsKey("msgThreshold")) {
            msgThreshold = 200000;
        } else {
            msgThreshold = jsonObject.getInteger("msgThreshold");
        }

        // 初始化 pathStorage 和 messageStorage
        pathStorages = new ArrayList<>();
        messageStorages = new ArrayList<>();
        Map<Integer, ThreadLocal<LocalThreadMessageStorage>> threadLocalMessageStoreMap = new HashMap<>();

        this.fragBatchRange = getFragBatchRange();

        for (int i = 0; i < fragBatchRange.size(); i++) {
            long[] startAndEnd = fragBatchRange.get(i);
            long beingVertex = startAndEnd[0];
            long endVertex = startAndEnd[1];
            pathStorages.add(new PathStorage(msgThreshold, getPath("vertex_atr", frag.fid(), beingVertex, endVertex),
                    i, beingVertex, endVertex));
            MessageStorage messageStorage = new MessageStorage(msgThreshold, getPath("received_path", frag.fid(), beingVertex, endVertex), beingVertex, endVertex);
            messageStorages.add(messageStorage);
            threadLocalMessageStoreMap.put(i, ThreadLocal.withInitial(() -> new LocalThreadMessageStorage(messageStorage)));
        }

        this.threadLocalMessageStorageFactory = new LocalThreadMessageStorageFactory(threadLocalMessageStoreMap);
        logger.info("disk context init end.msgThreshold is {},fragBatchRange is {}", msgThreshold, fragBatchRange.stream().map(startAndEnd -> startAndEnd[0] + "_" + startAndEnd[1]).collect(Collectors.toList()));
    }

    /**
     * {@code beingVertex inclusive,
     * endVertex exclusive}
     *
     * @return
     */
    private List<long[]> getFragBatchRange() {
        int verticesNumPerBatch = (int) (ivnum / fragBatchNum);

        List<long[]> rangeList = new ArrayList<>(fragBatchNum);
        for (int i = 0; i < fragBatchNum; ++i) {
            long beingVertex = (long) i * verticesNumPerBatch;
            long endVertex = Math.min((i + 1) * verticesNumPerBatch, ivnum);

            if (i == fragBatchNum - 1) {
                endVertex = ivnum;
            }
            rangeList.add(new long[]{beingVertex, endVertex});
        }

        return rangeList;
    }

    private String getPath(String prefix, int fid, long beginVertex, long endVertex) {
        return prefix + "_" + fid + "_" + beginVertex + "_" + endVertex;
    }

    public MessageStorage getMessageStorage(int batchId) {
        return messageStorages.get(batchId);
    }

    public LocalThreadMessageStorage getLocalMessageStorage(int batchId) {
        return threadLocalMessageStorageFactory.get(batchId);
    }

    public PathStorage getPathStorages(int batchId) {
        return pathStorages.get(batchId);
    }

    public int getBatchIdFromVertexId(long vertexId) {
        for (int i = 0; i < fragBatchRange.size(); i++) {
            long[] longs = fragBatchRange.get(i);
            long beginVertex = longs[0];
            long endVertex = longs[1];
            if (vertexId >= beginVertex && vertexId < endVertex) {
                return i;
            }
        }
        logger.error("getBatchIdFromVertexId error.vertexId is {}", vertexId);
        throw new RuntimeException("getBatchIdFromVertexId error.");
    }

    public void clearMemoryPathStorages() {
        for (PathStorage ps : pathStorages) {
            ps.clearInMemory();
        }
    }

    public void clearMessageStorages() {
        for (MessageStorage ms : messageStorages) {
            ms.clearInMemory();
            ms.clearInDisk();
        }
    }

    public void clearDiskPathStorages() {
        for (PathStorage ps : pathStorages) {
            ps.clearInDisk();
        }
    }

    public void clearDiskMessageStorages() {
        for (MessageStorage ms : messageStorages) {
            ms.clearInDisk();
        }
    }
}
