package com.alibaba.graphscope.example.circle.parallel.formal.store.disk;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 线程内的 MessageStore 工厂
 * 每个batch 对应一个线程内的 MessageStore
 *
 * @author liumin
 * @date 2024-08-07
 */
public class LocalThreadMessageStorageFactory {

    /**
     * MessageStore 按 batch 区分
     * key 为 batchId，value 为线程内存储的 MessageStore
     */
    private final Map<Integer, ThreadLocal<LocalThreadMessageStorage>> threadLocalMessageStoreMap;

    public LocalThreadMessageStorageFactory(Map<Integer, ThreadLocal<LocalThreadMessageStorage>> threadLocalMessageStoreMap) {
        this.threadLocalMessageStoreMap = threadLocalMessageStoreMap;
    }

    /**
     * 根据 batchId 获取线程内的某个 MessageStore
     *
     * @param batchId
     * @return
     */
    public LocalThreadMessageStorage get(int batchId) {
        return threadLocalMessageStoreMap.get(batchId)
                .get();
    }

    /**
     * 获取所有 batch 的线程内的 MessageStore
     *
     * @return
     */
    public List<LocalThreadMessageStorage> getAllValue() {
        return threadLocalMessageStoreMap.values().stream().map(ThreadLocal::get).collect(Collectors.toList());
    }

    /**
     * 移除所有 batch 对应的 thread local 变量
     */
    public void remove() {
        threadLocalMessageStoreMap.values().forEach(ThreadLocal::remove);
    }

    /**
     * 将线程内所有 batch 对应的 messageStore 持久化
     */
    public void dumpAll() {
        threadLocalMessageStoreMap.values().forEach(batchMessageStore -> batchMessageStore.get().dump());
    }
}
