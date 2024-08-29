package com.alibaba.graphscope.example.circle.parallel.formal;

import com.carrotsearch.hppc.ArraySizingStrategy;
import com.carrotsearch.hppc.BoundedProportionalArraySizingStrategy;
import com.carrotsearch.hppc.LongArrayList;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 环形 util
 * @author liumin
 * @date 2024-08-02
 */
public class CircleUtil {
    /**
     * 初始化 LongArrayList 使用的 resizeStrategy
     */
    public static final ArraySizingStrategy RESIZE_STRATEGY = new BoundedProportionalArraySizingStrategy(1, BoundedProportionalArraySizingStrategy.DEFAULT_MAX_GROW_COUNT, BoundedProportionalArraySizingStrategy.DEFAULT_GROW_RATIO);
    public static final String IDS_SEPARATOR = "&";
    public static final String PATH_SEPARATOR = "|";
    public static final int DEFAULT_INITIAL_LIST_SIZE = 4;
    public static final int MIN_CIRCLE_VERTEX_SIZE = 3;

    /**
     * 根据当前点的属性过滤，保留符合条件的路径：
     * <ol>
     *     <li> 点路径长度 >= 当前超步 + 1 </li>
     *     <li> 非环 </li>
     * </ol>
     *
     * @param vertexPath    点path
     * @param currIteration 当前超步 + 1
     * @return {@code 符合条件：true
     * 不符合条件：false}
     */
    public static boolean filterByAttr(LongArrayList vertexPath, long currIteration) {
        int vertexPathSize = vertexPath.size();

        boolean pathSizeCheck = vertexPathSize >= currIteration;
        boolean notCircle = !CircleUtil.isCircle(vertexPath);
        return pathSizeCheck && notCircle;
    }

    /**
     * 消息过滤条件，根据邻居，在点属性中筛选符合条件的数据
     * <ol>
     *     <li>路径以当前点结尾</li>
     *     <li>不包含子环。判断若加入 to 节点，路径是否包含子环，e.g. [1,2,3 + 2]</li>
     *     <li>不往回走。针对无向环，出边已走过的情况下，入边不往回走，e.g.[1,2 + 1]</li>
     * </ol>
     *
     * @param from       当前点id
     * @param to         待发送消息的点id
     * @param vertexPath 单条消息
     * @return {{@code true/false}}
     */
    public static boolean filterByNbr(long from, long to, LongArrayList vertexPath, boolean isDirected) {
        int vertexPathSize = vertexPath.size();

        boolean pathDataCheck = from == vertexPath.get(vertexPathSize - 1);
        boolean notContainInnerCircle = (vertexPathSize <= 1 && !vertexPath.contains(to)) || (vertexPathSize > 1 && !subListContain(vertexPath, 1, vertexPathSize, to));


        // 避免无向环 来回发送 1 -e1-> 2<-e1- 1
        // 不支持长度为 2 的无向环，1 -e1-> 2 <-e2- 1
        boolean undirectedCycleMsg = !isDirected && vertexPathSize == 2 && vertexPath.get(0) == to;

        return pathDataCheck
                // 避免子环 1 2 3 4 2
                && notContainInnerCircle && !undirectedCycleMsg;
    }

    /**
     * 子列表中是否包含指定value
     * {@code fromIndex}, inclusive, and {@code toIndex}, exclusive
     *
     * @param list      列表
     * @param fromIndex 起始索引 inclusive
     * @param toIndex   结束索引 exclusive
     * @param value     指定值
     * @return
     */
    private static boolean subListContain(LongArrayList list, int fromIndex, int toIndex, long value) {
        for (int i = fromIndex; i < toIndex; i++) {
            if (list.get(i) == value) {
                return true;
            }
        }
        return false;
    }

    /**
     * 从 点属性和关联边 中提取下一次迭代要发送的消息
     * 将符合条件的消息添加到 msg 集合
     *
     * @param superStep  超步
     * @param vertexPath 当前迭代的路径列表
     * @param from       当前点id
     * @param to         邻居点id
     * @param msg        消息列表
     * @param isDirected 是否有向，无向环不能往回发送消息
     */
    public static void makeMsgToSendAlongEdge(int superStep, Set<LongArrayList> vertexPath, long from, long to, List<LongArrayList> msg, boolean isDirected) {
        for (LongArrayList path : vertexPath) {
            if (CircleUtil.filterByAttr(path, superStep + 1) && CircleUtil.filterByNbr(from, to, path, isDirected)) {
                msg.add(path);
            }
        }
    }

    /**
     * 从 点属性和关联边 中提取下一次迭代要发送的消息，然后压缩消息
     * <ol>
     *     <li>压缩前需要过滤出符合条件的消息，避免压缩后生成无效消息</li>
     *     <li>消息压缩，避免单条消息过大。导致后续迭代OOM</li>
     * </ol>
     * 简单实现针对环的压缩
     * 1）图中所有点为种子节点，若路径上 点 均为种子节点，将路径首尾相同的，保留一条即可
     *
     * @param vertexPath 路径列表
     * @param msg        压缩后的路径列表
     */
    public static void makeAndCompressMsgToSendAlongEdge(int superStep, Set<LongArrayList> vertexPath, long from, long to, List<LongArrayList> msg, boolean isDirected) {
        // 将路径首尾相同的压缩
        Collection<LongArrayList> selectedPaths = vertexPath.stream()
                .filter(path -> CircleUtil.filterByAttr(path, superStep + 1)
                        && CircleUtil.filterByNbr(from, to, path, isDirected))
                .collect(Collectors.groupingBy(path -> path.get(0) + "," + path.get(path.size() - 1),
                        Collectors.collectingAndThen(Collectors.toList(),
                                grouped -> grouped.get(0))))
                .values();
        msg.addAll(selectedPaths);
    }

    /**
     * 路径是否为环
     * <ol>点数量 >=3 </ol>
     * <ol>起始节点 == 结束节点</ol>
     *
     * @param path 路径
     * @return 是否成环
     */
    public static boolean isCircle(LongArrayList path) {
        int size = path.size();
        return size >= MIN_CIRCLE_VERTEX_SIZE && path.get(0) == path.get(size - 1);
    }
}
