package com.alibaba.graphscope.example.circle.parallel.formal.store.disk;

import com.alibaba.graphscope.example.circle.parallel.formal.CircleAppParallelContext;
import com.alibaba.graphscope.example.circle.parallel.formal.PathSerAndDeser;
import com.alibaba.graphscope.example.circle.parallel.formal.store.ComputeStep;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
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

/**
 * 点属性文件存储
 *
 * @author liumin
 * @date 2024-08-07
 */
public class PathStorage extends FileObjectStorage {
    private static final Logger logger = LoggerFactory.getLogger(PathStorage.class);
    /**
     * 这里我们使用一个 Map 来存储一个 batch 内点的所有path
     */
    private Map<Integer, Set<LongArrayList>> fragmentPaths;
    private final int batchId;
    private final long beginVertex;
    private final long endVertex;
    private final List<LongArrayList> loadContainer;

    public Map<Integer, Set<LongArrayList>> getFragmentPaths() {
        return fragmentPaths;
    }

    public PathStorage(int msgThreshold, String path, int batchId, long beginVertex, long endVertex) {
        super(path, true, msgThreshold);
        this.fragmentPaths = new ConcurrentHashMap<>();
        this.batchId = batchId;
        this.beginVertex = beginVertex;
        this.endVertex = endVertex;
        this.loadContainer = new ArrayList<>();
    }

    public long getBeginVertex() {
        return beginVertex;
    }

    public long getEndVertex() {
        return endVertex;
    }

    public int getBatchId() {
        return batchId;
    }

    public Set<LongArrayList> getPath(long vid) {
        return fragmentPaths.computeIfAbsent((int) vid, k -> new HashSet<>());
    }

    public void putToStorage(int vid, List<LongArrayList> paths) {
        // paths 是重复使用的集合，不可直接 put 到 batchMessages。为优化对象创建，避免在此频繁 new Arraylist
        this.fragmentPaths.compute(vid, (k, v) -> {
            if (v == null) {
                return new HashSet<>(paths);
            }

            v.addAll(paths);
            return v;
        });
    }

    public void putToStorage(int vid, LongArrayList msg) {
        // paths 是重复使用的集合，不可直接 put 到 batchMessages。为优化对象创建，避免在此频繁 new Arraylist
        this.fragmentPaths.compute(vid, (k, paths) -> {
            if (paths == null) {
                Set<LongArrayList> result = new HashSet<>();
                result.add(msg);
                return result;
            }
            paths.add(msg);
            return paths;
        });
    }

    @Override
    public synchronized void clearInMemory() {
        for (Set<LongArrayList> l : fragmentPaths.values()) {
            for (LongArrayList path : l) {
                if (path != null) {
                    path.clear();
                }
            }
            l.clear();
        }
        fragmentPaths.clear();
    }

    public void clearInDisk() {
        clearInDisk("vertex_atr_");
    }

    @Override
    public void loadObjects(ObjectInputStream in) throws IOException {

        int vertexSize = in.readInt();
        // read from begin to end vertex
        for (int i = 0; i < vertexSize; ++i) {
            loadContainer.clear();

            int index = in.readInt();
            int pathNum = in.readInt();

            for (int j = 0; j < pathNum; ++j) {
                LongArrayList l = PathSerAndDeser.deserialize(in);
                if (l.isEmpty()) {
                    logger.info("PathS loadObjects.path size is 0");
                }
                loadContainer.add(l);
            }

            putToStorage(index, loadContainer);
        }
    }

    @Override
    public void dumpObjects(ObjectOutputStream out) {
        try {
            FileObjectStorage.dumpVertexObjects(out, this.fragmentPaths);
        } finally {
            clearInMemory();
        }
    }

    private boolean loadOrFlush() {
        return this.fragmentPaths.size() >= getMsgThreshold();
    }

    /**
     * 从 Path Store 加载消息，发送出去
     * 发送消息并行执行
     *
     * @param ctx     上下文
     * @param step    流程计算类
     * @param graph   子图
     * @param process 批次处理类
     * @param mm      并行消息管理器
     */
    public void loadObjectsAndSendMessage(CircleAppParallelContext ctx, ComputeStep step, IFragment<Long, Long, Long, Long> graph, IBatchProcess process, ParallelMessageManager mm) {
        long start = System.currentTimeMillis();
        if (!getFile().exists()) {
            return;
        }

        try (FileInputStream fileIn = new FileInputStream(getFile()); BufferedInputStream bufferedInputStream = new BufferedInputStream(fileIn, 1024); ObjectInputStream in = new ObjectInputStream(bufferedInputStream);) {

            clearInMemory();
            while (true) {
                if (loadOrFlush()) {
                    process.sendMsg(ctx, step, graph, this, mm);
                }
                try {
                    loadObjects(in);
                } catch (EOFException e) {
                    process.sendMsg(ctx, step, graph, this, mm);
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("load error", e);
        }
        logger.info("sendMessage end.superStep is {},batch is {},begin -> end is {},file is {},time is {}s", ctx.superStep, batchId, getBeginVertex() + "->" + getEndVertex(), getPath(), (System.currentTimeMillis() - start) / 1000);
    }
}