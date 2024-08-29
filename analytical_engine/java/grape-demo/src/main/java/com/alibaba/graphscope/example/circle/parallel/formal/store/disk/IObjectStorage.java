package com.alibaba.graphscope.example.circle.parallel.formal.store.disk;

/**
 * 文件读写接口
 *
 * @author liumin
 * @date 2024/8/21
 */
public interface IObjectStorage {
    /**
     * 加载文件
     */
    void load();

    /**
     * 文件写入
     */
    void dump();

    /**
     * 清理内存数据
     */
    void clearInMemory();
}

