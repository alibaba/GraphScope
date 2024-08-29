package com.alibaba.graphscope.example.circle.parallel.formal.store.disk;

import com.alibaba.graphscope.example.circle.parallel.formal.CircleAppParallelContext;
import com.alibaba.graphscope.example.circle.parallel.formal.PathSerAndDeser;
import com.alibaba.graphscope.fragment.IFragment;
import com.carrotsearch.hppc.LongArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 消息文件存储
 *
 * @author liumin
 * @date 2024-08-07
 */
public class MessageStorage extends FileObjectStorage {
    private static final Logger logger = LoggerFactory.getLogger(MessageStorage.class);
    private final Map<Integer, Set<LongArrayList>> batchMessages;
    private final long beginVertex;
    private final long endVertex;
    private final List<LongArrayList> loadContainer;
    private final ReadWriteLock lock;

    public MessageStorage(int msgThreshold, String path, long beginVertex, long endVertex) {
        super(path, true, msgThreshold);
        this.batchMessages = new ConcurrentHashMap<>();
        this.beginVertex = beginVertex;
        this.endVertex = endVertex;
        this.loadContainer = new ArrayList<>();
        this.lock = new ReentrantReadWriteLock();
    }

    public ReadWriteLock getLock() {
        return lock;
    }

    public Map<Integer, Set<LongArrayList>> getBatchMessages() {
        return batchMessages;
    }


    /**
     * 将消息更新到 MessageStore，相同 Key 的消息合并成一条
     *
     * @param lid  内部点自增id
     * @param msgs 消息集合
     */
    public void putToStorage(int lid, List<LongArrayList> msgs) {
        // msgs 是重复使用的集合，不可直接 put 到 batchMessages。为优化对象创建，避免在此频繁 new Arraylist
        this.batchMessages.compute(lid, (k, storedMsgs) -> {
            if (storedMsgs == null) {
                return new HashSet<>(msgs);
            }
            storedMsgs.addAll(msgs);
            return storedMsgs;
        });
    }

    private boolean loadOrFlush() {
        return this.batchMessages.size() >= getMsgThreshold();
    }

    /**
     * 读取 batch 的所有消息
     *
     * @param in
     */
    @Override
    public void loadObjects(ObjectInputStream in) throws IOException {

        int vertexSize = in.readInt();
        // read from begin to end vertex
        for (int i = 0; i < vertexSize; ++i) {
            loadContainer.clear();

            int index = in.readInt();
            int pathNum = in.readInt();
            //List<LongArrayList> list = new ArrayList<>(pathNum);
            for (int j = 0; j < pathNum; ++j) {
                LongArrayList l = PathSerAndDeser.deserialize(in);
                if (l.isEmpty()) {
                    logger.info("MessS loadObjects.path size is 0");
                }
                loadContainer.add(l);
            }

            putToStorage(index, loadContainer);
        }
    }

    /**
     * 写入一条消息
     *
     * @param out 输出字节流
     */
    @Override
    public void dumpObjects(ObjectOutputStream out) {
        try {
            FileObjectStorage.dumpVertexObjects(out, this.batchMessages);
        } finally {
            clearInMemory();
        }
    }

    public boolean isEmpty() {
        return this.batchMessages.isEmpty();
    }

    public int size() {
        return this.batchMessages.size();
    }

    @Override
    public void clearInMemory() {
        for (Set<LongArrayList> msg : this.batchMessages.values()) {
            msg.clear();
        }

        this.batchMessages.clear();
    }

    public void clearInDisk() {
        clearInDisk("received_path_");
    }

    @Override
    public String toString() {
        return "MessageStorage{" + "batchMessages=" + batchMessages + '}';
    }

    /**
     * 从 Message Store 加载消息更新到 Path Store
     * 串行执行
     * TODO：若消息数量太多，可优化成并行
     *
     * @param ctx
     * @param process
     * @param graph
     * @param batchId
     */
    public void loadInBatchAndUpdateStorage(CircleAppParallelContext ctx, IBatchProcess process, IFragment<Long, Long, Long, Long> graph, int batchId) {
        long start = System.currentTimeMillis();
        if (!getFile().exists()) {
            return;
        }
        // 获取 点属性 storage
        PathStorage pathStorage = ctx.diskStoreContext.getPathStorages(batchId);
        try (FileInputStream fileIn = new FileInputStream(getFile());
             BufferedInputStream bufferedInputStream = new BufferedInputStream(fileIn, 1024);
             ObjectInputStream in = new ObjectInputStream(bufferedInputStream);) {

            clearInMemory();
            while (true) {
                if (loadOrFlush()) {
                    process.batchUpdatePathStorage(ctx, graph, this, pathStorage);
                }
                try {
                    loadObjects(in);
                } catch (EOFException e) {
                    process.batchUpdatePathStorage(ctx, graph, this, pathStorage);
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("load error", e);
        }
        logger.info("batchUpdatePathStorage end.superStep is {},batch is {},begin -> end is {},file is {},time is {}s", ctx.superStep, batchId, beginVertex + "->" + endVertex, getPath(), (System.currentTimeMillis() - start) / 1000);
    }
}
