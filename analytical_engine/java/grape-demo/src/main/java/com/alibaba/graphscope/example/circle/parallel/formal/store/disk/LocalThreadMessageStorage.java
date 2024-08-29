package com.alibaba.graphscope.example.circle.parallel.formal.store.disk;

import com.carrotsearch.hppc.LongArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * 线程安全的LocalThreadMessageStorage,通过ThreadLocal 存储 MessageStorage
 * <p>使用 {{{@link LocalThreadMessageStorageFactory}}} 初始化
 * <p>使用场景：多线程更新及清空 batchMessages
 * <p>不同 LocalThreadMessageStorage 写入同一个 MessageStorage对应的磁盘文件
 *
 * @author liumin
 * @date 2024-08-07
 */
public class LocalThreadMessageStorage extends FileObjectStorage {
    private static final Logger logger = LoggerFactory.getLogger(LocalThreadMessageStorage.class);
    private final Map<Integer, Set<LongArrayList>> batchMessages;
    private final MessageStorage messageStorage;

    public LocalThreadMessageStorage(MessageStorage messageStorage) {
        super(messageStorage.getPath(), messageStorage.isAppend(), messageStorage.getMsgThreshold());
        this.messageStorage = messageStorage;
        this.batchMessages = new ConcurrentHashMap<>();
    }

    public MessageStorage getMessageStorage() {
        return messageStorage;
    }

    public void putToLocalStorage(int lid, List<LongArrayList> msgs) {
        // msgs 是重复使用的集合，不可直接 put 到 batchMessages。为优化对象创建，避免在此频繁 new Arraylist
        this.batchMessages.compute(lid, (k, storedMsgs) -> {
            if (storedMsgs == null) {
                return new HashSet<>(msgs);
            }
            storedMsgs.addAll(msgs);
            return storedMsgs;
        });
    }

    /**
     * 按批次并发处理消息并写入对应 messageStore 文件
     * @param toLid
     * @param msg
     */
    public void batchDumpMessages(int toLid, List<LongArrayList> msg) {
        putToLocalStorage(toLid, msg);
        if (loadOrFlush()) {
            dump();
        }
    }

    /**
     * 根据收到的消息数量判断是否flush
     *
     * @return
     */
    private boolean loadOrFlush() {
        return this.batchMessages.size() >= messageStorage.getMsgThreshold();
    }

    /**
     * 顺序写同一个 messageStore 文件
     * 一个 messageStore 对应一个文件
     */
    @Override
    public void dump() {
        ReadWriteLock lock = messageStorage.getLock();
        try {
            lock.writeLock().lock();
            super.dump();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 清理 local thread 的 batchMessages
     */
    @Override
    public void clearInMemory() {
        for (Set<LongArrayList> msg : this.batchMessages.values()) {
            msg.clear();
        }

        this.batchMessages.clear();
    }

    @Override
    public String toString() {
        return "MessageStorage{" + "batchMessages=" + batchMessages + '}';
    }

    @Override
    public void loadObjects(ObjectInputStream in) throws IOException {
        // 加载 messageStore 磁盘文件
        messageStorage.loadObjects(in);
    }

    @Override
    public void dumpObjects(ObjectOutputStream out) {
        try {
            FileObjectStorage.dumpVertexObjects(out, this.batchMessages);
        } finally {
            clearInMemory();
        }
    }
}
