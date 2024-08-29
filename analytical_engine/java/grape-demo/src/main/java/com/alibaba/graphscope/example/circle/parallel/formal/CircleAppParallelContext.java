package com.alibaba.graphscope.example.circle.parallel.formal;

import com.alibaba.graphscope.example.circle.parallel.formal.store.ComputeStep;
import com.alibaba.graphscope.example.circle.parallel.formal.store.disk.DiskComputeStepImpl;
import com.alibaba.graphscope.example.circle.parallel.formal.store.disk.DiskStoreContext;
import com.alibaba.graphscope.example.circle.parallel.formal.store.memory.MemoryComputeStepImpl;
import com.alibaba.graphscope.utils.ThreadSafeBitSet;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.StdString;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

/**
 * App Context
 * 不支持 用户自定义的 点属性类型
 * VertexDataContext 仅支持String Primitive(boolean、int、long)等简单的数据类型
 *
 * @author liumin
 * @date 2024-07-12
 */
public class CircleAppParallelContext extends VertexDataContext<IFragment<Long, Long, Long, Long>, StdString> implements ParallelContextBase<Long, Long, Long, Long> {
    private static final Logger logger = LoggerFactory.getLogger(CircleAppParallelContext.class);
    /**
     * superStep超步
     */
    public int superStep;
    /**
     * 最小迭代次数
     */
    public int minIteration;
    /**
     * 最大迭代次数
     */
    public int maxIteration;
    /**
     * 并发接收/发送消息的线程数
     */
    public int threadNum;
    public ExecutorService executor;
    private static final int NO_BATCH = -2;
    /**
     * 种子节点执行的批次数
     */
    public int batchNum;

    /**
     * 存在多线程 对同一个点 处理的情况，避免并发问题
     * 多个点 同时 发消息给 同一个点
     * <p>
     * 当前超步所有路径列表
     */
    public Map<Integer, Set<LongArrayList>> currentPaths;

    /**
     * 当前超步，frag 内部 inner vertex 之间互相发送消息保存的消息列表
     * <p>
     * 下一超步使用
     */
    public Map<Integer, Set<LongArrayList>> nextPaths;

    /**
     * 存储环形结果
     */
    public Map<Integer, List<LongArrayList>> circleResult;

    /**
     * 当前超步活跃的节点，包含：
     * 1）接收到 msg 的 vertex 集合
     * 2）上一个迭代 send to local nbr 对应的 nbr
     */
    public ThreadSafeBitSet currModified;
    /**
     * 当前超步更新的 frag 内部的点集合，下一超步活跃的节点：
     * 1）send to local nbr 对应的 nbr
     */
    public ThreadSafeBitSet nextModified;

    /**
     * 环形是否有向
     */
    public boolean isDirected;

    /**
     * 消息Stream,每个线程维护一个 FFIByteVectorOutputStream，避免并发问题
     * Key 为线程id，Value 为消息 Stream
     */
    public ThreadLocal<FFIByteVectorOutputStream> msgVectorMap;

    /**
     * 反序列化后的消息集合
     * Key 为线程id，Value 为 传播的路径列表
     */
    public ThreadLocal<List<LongArrayList>> deserializedMsgMap;

    /**
     * 发送消息前，合并多边后使用的集合
     * Key 为线程id，Value 为Map: key 为 nbr id，value 为邻居节点
     */
    public ThreadLocal<Map<Long, Vertex<Long>>> nbrMap;

    /**
     * memory or disk 处理器
     */
    public ComputeStep storeProcessor;

    /**
     * disk 模式 context
     */
    public DiskStoreContext diskStoreContext;

    /**
     * 是否所有节点均为种子节点
     */
    public boolean allSeed = true;
    /**
     * 消息 send 单个点上的路径阈值，超过阈值则压缩
     */
    public int pathThreshold;

    private static final int DEFAULT_MIN_ITERATION = 2;
    private static final int UNDIRECTED_DEFAULT_MIN_ITERATION = 3;

    @Override
    public void Init(IFragment<Long, Long, Long, Long> frag, ParallelMessageManager messageManager, JSONObject jsonObject) {
        // Output 设置
        createFFIContext(frag, StdString.class, false);

        // initial variable，初始 size 和 线程数量相等
        msgVectorMap = ThreadLocal.withInitial(FFIByteVectorOutputStream::new);
        deserializedMsgMap = ThreadLocal.withInitial(Lists::newArrayList);
        nbrMap = ThreadLocal.withInitial(HashMap::new);

        superStep = 0;

        // 是否有向
        if (!jsonObject.containsKey("isDirected")) {
            isDirected = true;
        } else {
            isDirected = jsonObject.getBoolean("isDirected");
        }
        // 最小迭代次数
        if (!jsonObject.containsKey("minIteration")) {
            minIteration = DEFAULT_MIN_ITERATION;
        } else {
            minIteration = jsonObject.getInteger("minIteration");
        }

        if (!isDirected && minIteration < UNDIRECTED_DEFAULT_MIN_ITERATION) {
            // 无向最小环大小为 3
            minIteration = UNDIRECTED_DEFAULT_MIN_ITERATION;
        }

        // 最大迭代次数
        if (!jsonObject.containsKey("maxIteration")) {
            maxIteration = 3;
        } else {
            maxIteration = jsonObject.getInteger("maxIteration");
        }

        // 线程数
        if (!jsonObject.containsKey("threadNum")) {
            threadNum = 2;
        } else {
            threadNum = jsonObject.getInteger("threadNum");
        }
        // 路径数量阈值
        if (!jsonObject.containsKey("pathThreshold")) {
            pathThreshold = 10000;
        } else {
            pathThreshold = jsonObject.getInteger("pathThreshold");
        }
        // 初始化线程池
        ThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("Graphscope" + "-%d").build();
        executor = new ThreadPoolExecutor(threadNum, threadNum, 60L, java.util.concurrent.TimeUnit.SECONDS, new java.util.concurrent.LinkedBlockingQueue<>(100000), threadFactory);

        /**
         *  批次数,拆分种子节点分多批次执行
         *  start from 0
         */
        if (!jsonObject.containsKey("batchNum")) {
            batchNum = NO_BATCH;
        } else {
            batchNum = jsonObject.getInteger("batchNum");
        }

        long innerVertexNum = frag.getInnerVerticesNum();

        // 初始化容量大小等于 frag 内部节点数
        currentPaths = new ConcurrentHashMap<>((int) innerVertexNum);
        nextPaths = new ConcurrentHashMap<>((int) innerVertexNum);
        circleResult = new ConcurrentHashMap<>((int) innerVertexNum);

        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();

        // 遍历 frag 内部所有点，初始化种子节点
        for (long i = 0; i < innerVertexNum; ++i) {
            vertex.setValue(i);

            long oid = frag.getId(vertex);
            // 点属性 表示批次数
            long vertexData = frag.getData(vertex);
            // 非种子节点跳过
            if (vertexData == -1) {
                allSeed = false;
                continue;
            }
            if (batchNum == NO_BATCH || batchNum == vertexData) {
                initSeedVertex((int) i, oid);
            }
        }

        currModified = new ThreadSafeBitSet(ThreadSafeBitSet.DEFAULT_LOG2_SEGMENT_SIZE_IN_BITS, (int) frag.getInnerVerticesNum());
        nextModified = new ThreadSafeBitSet(ThreadSafeBitSet.DEFAULT_LOG2_SEGMENT_SIZE_IN_BITS, (int) frag.getInnerVerticesNum());

        /**
         * 计算中间数据存储级别
         * <ol>
         *     <li>memory</li>
         *     <li>disk</li>
         * </ol>
         */
        String storageLevel = !jsonObject.containsKey("storageLevel") ? "memory" : jsonObject.getString("storageLevel");
        if (storageLevel.equals("memory")) {
            storeProcessor = new MemoryComputeStepImpl(this);
        } else if (storageLevel.equals("disk")) {
            diskStoreContext = new DiskStoreContext(frag, jsonObject);
            storeProcessor = new DiskComputeStepImpl(this);
        } else {
            throw new IllegalArgumentException("storageLevel only can be [memory ｜ disk]");
        }

        messageManager.initChannels(threadNum);

        logger.info("init end.innerVertexNum is {},minIteration is {},maxIteration is {},batchNum is {},isDirected is {},threadNum is {},active vertexNum is {},allSeed is {}", innerVertexNum, minIteration, maxIteration, batchNum, isDirected, threadNum, currentPaths.size(), allSeed);
    }

    /**
     * 初始化种子节点
     *
     * @param lid 内部自增点id
     * @param oid 原始点id
     */
    private void initSeedVertex(int lid, long oid) {
        // initial vertex condition,init path with new list
        LongArrayList path = new LongArrayList(CircleUtil.DEFAULT_INITIAL_LIST_SIZE, CircleUtil.RESIZE_STRATEGY);
        path.add(oid);
        Set<LongArrayList> initialAttr = new HashSet<>();
        initialAttr.add(path);
        this.currentPaths.put(lid, initialAttr);
    }

    public Set<LongArrayList> getCurrentPaths(int lid) {
        return currentPaths.get(lid);
    }

    public Set<LongArrayList> getCurrentPaths(Vertex<Long> vertex) {
        return getCurrentPaths(vertex.getValue().intValue());
    }

    /**
     * 单个点消息发送结束后，该点在当前超步内存中的数据可以被清理掉
     * 以下场景执行该内存清理操作有效：
     * <ol>
     *     <li>memory 模式</li>
     *     <li>disk 模式下，superStep=0</li>
     * </ol>
     *
     * @param vertex 需要被清理的点
     */
    public void clearVertexInMemory(Vertex<Long> vertex) {
        this.currentPaths.compute(vertex.getValue().intValue(), (k, path) -> {
            if (path != null && !path.isEmpty()) {
                path.clear();
            }
            return path;
        });
    }

    public List<LongArrayList> getPathListObject() {
        return deserializedMsgMap.get();
    }

    public FFIByteVectorOutputStream getMsgVectorObject() {
        return msgVectorMap.get();
    }

    public Map<Long, Vertex<Long>> getNbrMapObject() {
        return nbrMap.get();
    }

    @Override
    public void Output(IFragment<Long, Long, Long, Long> frag) {
        GSVertexArray<StdString> vertexArray = data();
        Vertex<Long> cur = FFITypeFactoryhelper.newVertexLong();
        for (long vid = 0; vid < frag.getInnerVerticesNum(); ++vid) {
            cur.setValue(vid);

            StdString value = (StdString) vertexArray.get(cur);
            if (circleResult.get((int) vid) != null) {
                value.fromJavaString(outPutCircleResult((int) vid));
            }
        }

        clean();
    }

    private String outPutCircleResult(int lid) {
        // 输出到 ctx，仅输出成环结果
        List<String> pathList = this.circleResult.get(lid).stream().filter(path -> CircleUtil.isCircle(path) && path.size() > minIteration).map(this::outputToCtx).collect(Collectors.toList());
        if (!pathList.isEmpty()) {
            return StringUtils.join(pathList, CircleUtil.PATH_SEPARATOR);
        }
        return "";
    }

    private String outputToCtx(LongArrayList path) {
        StringBuffer sb = new StringBuffer();
        int size = path.size();
        for (int i = 0; i < size; i++) {
            long id = path.get(i);
            sb.append(id);
            if (i < size - 1) {
                sb.append(CircleUtil.IDS_SEPARATOR);
            }
        }

        return sb.toString();
    }

    /**
     * 内存模式下，交换当前超步的路径和下一次超步的路径
     */
    public void swapPaths() {
        Map<Integer, Set<LongArrayList>> tmp = currentPaths;
        currentPaths = nextPaths;
        nextPaths = tmp;

        nextPaths.clear();
    }

    /**
     * add to current path
     * 数据来源: 1. received msg  2.next paths
     *
     * @param lid      点的 local id
     * @param addPaths 新增的路径列表
     */
    public void addPathToCurrentPathsAndFindCircle(int lid, List<LongArrayList> addPaths) {
        // lazy initialize
        addPathToCtxAndFindCircle(lid, addPaths, this.currentPaths);
    }

    /**
     * 将 环形 结果添加到结果集合
     * from current Paths
     *
     * @param lid  点的 local id
     * @param path 新增路径
     */
    public void addPathToCircleResult(int lid, LongArrayList path) {
        // lazy initialize
        this.circleResult.compute(lid, (k, v) -> {
            if (v == null) {
                v = new ArrayList<>();
            }
            v.add(path);
            return v;
        });
    }

    /**
     * local nbr update,add to next paths
     *
     * @param lid      点id
     * @param addPaths 新增的路径列表
     */
    public void addPathToNextPathsAndFindCircle(int lid, List<LongArrayList> addPaths) {
        addPathToCtxAndFindCircle(lid, addPaths, this.nextPaths);
    }

    private void addPathToCtxAndFindCircle(int lid, List<LongArrayList> addPaths, Map<Integer, Set<LongArrayList>> ctxPaths) {
        // lazy initialize
        ctxPaths.compute(lid, (id, nextPath) -> {
            if (nextPath == null) {
                nextPath = new HashSet<>();
            }

            for (LongArrayList path : addPaths) {
                if (CircleUtil.isCircle(path)) {
                    if (path.size() > minIteration) {
                        addPathToCircleResult(id, path);
                    }
                    continue;
                }

                nextPath.add(path);
            }
            return nextPath;
        });
    }

    private void clean() {
        executor.shutdown();
    }

    /**
     * 移除 thread local 变量
     * 均在 send msg 的时候使用
     */
    public void removeTheadLocalVariable() {
        this.deserializedMsgMap.remove();
        this.msgVectorMap.remove();
        this.nbrMap.remove();
    }
}
