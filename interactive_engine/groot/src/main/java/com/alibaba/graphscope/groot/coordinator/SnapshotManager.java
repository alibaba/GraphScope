/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.SnapshotListener;
import com.alibaba.graphscope.groot.meta.MetaStore;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.CoordinatorConfig;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import com.alibaba.maxgraph.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.groot.wal.LogReader;
import com.alibaba.graphscope.groot.wal.LogService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SnapshotManager runs on a central node, e.g. Master/Coordinator. It is the core component of the
 * snapshot protocol. SnapshotManager is designed for the following purpose: - increase
 * writeSnapshotId, and broadcast to all Frontend nodes - accumulates ingest progresses from the
 * WriteAgents, and calculate the available querySnapshotId, then broadcast querySnapshotId to all
 * Frontend nodes
 *
 * <p>There are two parameters we need to decide: Frontend nodes count and broadcast interval.
 *
 * <p>For most cases, Frontend nodes won't be more than 100. I think we can assume the maximum count
 * of Frontend nodes is 256, and use 256 Frontend nodes in the stress test.
 *
 * <p>The broadcast interval affects the time user have to wait for querying the data after {@link
 * com.alibaba.maxgraph.frontendservice.realtime.RealtimeWriter#writeOperations(OperationTxn)}
 * returns successfully. Shorter broadcast interval means less time to wait, but that leads to
 * heavier load of SnapshotManager, since SnapshotManager need to broadcast writeSnapshotId /
 * querySnapshotId to all the Frontend nodes at the fixed interval.
 *
 * <p>The default broadcast interval can be set to 1 second. User can increase the interval to
 * reduce the load of SnapshotManager. If user want a shorter interval, I will suggest we provide a
 * sync write interface rather than decrease the interval to milliseconds. The general idea is to
 * let SnapshotManager broadcast new writeSnapshotId to Frontend nodes on demand for syncWrite,
 * instead of consistently broadcast new writeSnapshotId in high frequency. I will briefly introduce
 * the syncWrite implementation below.
 *
 * <p>When RealtimeWriter receives a syncWrite call, it invokes {@link
 * com.alibaba.maxgraph.frontendservice.realtime.RealtimeWriter#writeOperations(OperationTxn)} same
 * as asyncWrite and get the returned snapshotId, e.g. s0. Then RealtimeWriter registers a
 * SnapshotListener with s0 to the SnapshotManager, and blocking on the listener callback.
 * SnapshotManager will cache the listener and immediately broadcast a new writeSnapshotId to the
 * Frontend nodes. When SnapshotManager knows the available querySnapshotId is greater than or equal
 * to s0, it first broadcasts new querySnapshotId to all Frontend nodes, then triggers the listener
 * callback to complete the syncWrite call.
 *
 * <p>---------------------------------------------------------------------------------------------------------------
 *
 * <p>Another important issue is failure recovery.
 *
 * <p>We assume that there is a distributed reliable KV store, e.g. ZooKeeper, for recovery
 * information persistence.
 *
 * <p>The information need to persist are: - last generated writeSnapshotId - last available
 * querySnapshotId - consumed snapshotId of each GraphNode // not necessary? - consumed offset of
 * each queue on each GraphNode
 *
 * <p>When and in what frequency should we persist these information: - "last generated
 * writeSnapshotId" and "last available querySnapshotId" are the same. They must be persisted before
 * they are broadcast to the Frontend nodes. Otherwise, the system might step backward after process
 * recovery, which is not acceptable.
 *
 * <p>- "consumed snapshotId of each GraphNode" and "consumed offset of each queue on each
 * GraphNode" can be persisted asynchronously. We can endure lost the latest value of these
 * information. The only drawback is that the data replay might process some duplicate data, which
 * is acceptable.
 *
 * <p>After persist snapshot information as described above, the recovery process will be simply
 * load the persisted information from the reliable KV store when initializing the SnapshotManager.
 */
public class SnapshotManager {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    public static final String WRITE_SNAPSHOT_ID_PATH = "write_snapshot_id";
    public static final String QUERY_SNAPSHOT_INFO_PATH = "query_snapshot_info";
    public static final String QUEUE_OFFSETS_PATH = "queue_offsets";

    private MetaStore metaStore;
    private LogService logService;
    private WriteSnapshotIdNotifier writeSnapshotIdNotifier;

    private int storeCount;
    private int queueCount;
    private long snapshotIncreaseIntervalMs;
    private long offsetsPersistIntervalMs;

    private volatile SnapshotInfo querySnapshotInfo;
    private volatile long writeSnapshotId;

    private Map<Integer, SnapshotInfo> storeToSnapshotInfo;
    private Map<Integer, List<Long>> storeToOffsets;
    private AtomicReference<List<Long>> queueOffsetsRef;

    private ScheduledExecutorService increaseWriteSnapshotIdScheduler;
    private ScheduledExecutorService persistOffsetsScheduler;

    private List<QuerySnapshotListener> listeners = new CopyOnWriteArrayList<>();
    private TreeMap<Long, List<SnapshotListener>> snapshotToListeners = new TreeMap<>();

    private Object querySnapshotLock = new Object();
    private Lock writeSnapshotLock = new ReentrantLock();

    private ObjectMapper objectMapper;

    public SnapshotManager(
            Configs configs,
            MetaStore metaStore,
            LogService logService,
            WriteSnapshotIdNotifier writeSnapshotIdNotifier) {
        this.metaStore = metaStore;
        this.logService = logService;
        this.writeSnapshotIdNotifier = writeSnapshotIdNotifier;

        this.objectMapper = new ObjectMapper();
        this.queueCount = CommonConfig.INGESTOR_QUEUE_COUNT.get(configs);
        this.storeCount = CommonConfig.STORE_NODE_COUNT.get(configs);

        this.snapshotIncreaseIntervalMs =
                CoordinatorConfig.SNAPSHOT_INCREASE_INTERVAL_MS.get(configs);
        this.offsetsPersistIntervalMs = CoordinatorConfig.OFFSETS_PERSIST_INTERVAL_MS.get(configs);

        this.storeToSnapshotInfo = new ConcurrentHashMap<>();
        this.storeToOffsets = new ConcurrentHashMap<>();
    }

    public void start() {
        try {
            recover();
        } catch (IOException e) {
            throw new MaxGraphException(e);
        }

        this.increaseWriteSnapshotIdScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "increase-write-snapshot-scheduler", logger));
        this.increaseWriteSnapshotIdScheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        long snapshotId = increaseWriteSnapshotId();
                        logger.debug("writeSnapshotId updated to [" + snapshotId + "]");
                    } catch (Exception e) {
                        logger.error("error in increaseWriteSnapshotId, ignore", e);
                    }
                },
                0L,
                snapshotIncreaseIntervalMs,
                TimeUnit.MILLISECONDS);
        this.persistOffsetsScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "persist-offsets-scheduler", logger));
        this.persistOffsetsScheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        updateQueueOffsets();
                    } catch (Exception e) {
                        logger.error("error in updateQueueOffsets, ignore", e);
                    }
                },
                offsetsPersistIntervalMs,
                offsetsPersistIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (this.persistOffsetsScheduler != null) {
            this.persistOffsetsScheduler.shutdown();
            try {
                this.persistOffsetsScheduler.awaitTermination(3000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
            this.persistOffsetsScheduler = null;
        }
        if (this.increaseWriteSnapshotIdScheduler != null) {
            this.increaseWriteSnapshotIdScheduler.shutdown();
            try {
                this.increaseWriteSnapshotIdScheduler.awaitTermination(3000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
            this.increaseWriteSnapshotIdScheduler = null;
        }
    }

    private void checkMetaPath(String path) throws FileNotFoundException {
        if (!this.metaStore.exists(path)) {
            throw new FileNotFoundException(path);
        }
    }

    private void recover() throws IOException {
        checkMetaPath(QUERY_SNAPSHOT_INFO_PATH);
        checkMetaPath(WRITE_SNAPSHOT_ID_PATH);
        checkMetaPath(QUEUE_OFFSETS_PATH);

        byte[] querySnapshotInfoBytes = this.metaStore.read(QUERY_SNAPSHOT_INFO_PATH);
        SnapshotInfo recoveredQuerySnapshotInfo =
                this.objectMapper.readValue(querySnapshotInfoBytes, SnapshotInfo.class);

        byte[] writeSnapshotIdBytes = this.metaStore.read(WRITE_SNAPSHOT_ID_PATH);
        long recoveredWriteSnapshotId =
                this.objectMapper.readValue(writeSnapshotIdBytes, Long.class);

        if (recoveredQuerySnapshotInfo.getSnapshotId() > recoveredWriteSnapshotId) {
            throw new IllegalStateException(
                    "recovered querySnapshotInfo ["
                            + recoveredQuerySnapshotInfo
                            + "] > writeSnapshotId ["
                            + recoveredWriteSnapshotId
                            + "]");
        }

        byte[] queueOffsetsBytes = this.metaStore.read(QUEUE_OFFSETS_PATH);
        List<Long> recoveredQueueOffsets =
                this.objectMapper.readValue(queueOffsetsBytes, new TypeReference<List<Long>>() {});
        if (recoveredQueueOffsets.size() != this.queueCount) {
            throw new IllegalStateException(
                    "recovered queueCount ["
                            + recoveredQueueOffsets.size()
                            + "], but expect queueCount ["
                            + this.queueCount
                            + "]");
        }

        for (int i = 0; i < this.queueCount; i++) {
            long recoveredOffset = recoveredQueueOffsets.get(i);
            LogReader reader = null;
            try {
                reader = logService.createReader(i, recoveredOffset + 1);
            } catch (Exception e) {
                throw new IOException(
                        "recovered queue ["
                                + i
                                + "] offset ["
                                + recoveredOffset
                                + "] is not available",
                        e);
            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        }

        this.querySnapshotInfo = recoveredQuerySnapshotInfo;
        this.writeSnapshotId = recoveredWriteSnapshotId;
        this.queueOffsetsRef = new AtomicReference<>(recoveredQueueOffsets);
    }

    /**
     * WriterAgent use this method to commit snapshot information to SnapshotManager
     *
     * @param storeId
     * @param snapshotId
     * @param ddlSnapshotId
     * @param queueOffsets
     */
    public synchronized void commitSnapshotId(
            int storeId, long snapshotId, long ddlSnapshotId, List<Long> queueOffsets) {
        this.storeToSnapshotInfo.compute(
                storeId,
                (k, v) ->
                        (v != null && v.getSnapshotId() >= snapshotId)
                                ? v
                                : new SnapshotInfo(snapshotId, ddlSnapshotId));
        this.storeToOffsets.compute(
                storeId,
                (k, v) -> {
                    if (v != null) {
                        if (v.size() != queueOffsets.size()) {
                            throw new IllegalArgumentException(
                                    "current offset size ["
                                            + v.size()
                                            + "], commit offset size ["
                                            + queueOffsets.size()
                                            + "]");
                        }
                        for (int i = 0; i < v.size(); i++) {
                            if (v.get(i) > queueOffsets.get(i)) {
                                return v;
                            }
                        }
                    }
                    return queueOffsets;
                });
        maybeUpdateQuerySnapshotId();
    }

    public void addSnapshotListener(long snapshotId, SnapshotListener snapshotListener) {
        synchronized (this.querySnapshotLock) {
            if (querySnapshotInfo.getSnapshotId() >= snapshotId) {
                snapshotListener.onSnapshotAvailable();
                return;
            }
            List<SnapshotListener> snapshotListeners =
                    this.snapshotToListeners.computeIfAbsent(snapshotId, k -> new ArrayList<>());
            snapshotListeners.add(snapshotListener);
        }
    }

    public void addListener(QuerySnapshotListener listener) {
        this.listeners.add(listener);
        SnapshotInfo querySnapshotInfo = this.querySnapshotInfo;
        try {
            listener.snapshotAdvanced(
                    querySnapshotInfo.getSnapshotId(), querySnapshotInfo.getDdlSnapshotId());
        } catch (Exception e) {
            logger.error("error occurred when notify listeners", e);
        }
    }

    public void removeListener(QuerySnapshotListener listener) {
        this.listeners.remove(listener);
    }

    private void maybeUpdateQuerySnapshotId() {
        if (this.storeToSnapshotInfo.size() < this.storeCount) {
            logger.warn(
                    "Not all store nodes reported snapshot progress. current storeToSnapshot ["
                            + this.storeToSnapshotInfo
                            + "]");
            return;
        }
        SnapshotInfo minSnapshotInfo = Collections.min(this.storeToSnapshotInfo.values());
        if (minSnapshotInfo.getSnapshotId() > this.querySnapshotInfo.getSnapshotId()) {
            synchronized (this.querySnapshotLock) {
                long snapshotId = minSnapshotInfo.getSnapshotId();
                long ddlSnapshotId = minSnapshotInfo.getDdlSnapshotId();
                long currentSnapshotId = this.querySnapshotInfo.getSnapshotId();
                long currentDdlSnapshotId = this.querySnapshotInfo.getDdlSnapshotId();
                if (snapshotId > currentSnapshotId) {
                    try {
                        if (ddlSnapshotId < currentDdlSnapshotId) {
                            // During failover, store might send smaller ddlSnapshotId
                            minSnapshotInfo = new SnapshotInfo(snapshotId, currentDdlSnapshotId);
                            //                            throw new
                            // IllegalStateException("minSnapshotInfo [" + minSnapshotInfo +
                            //                                    "], currentSnapshotInfo [" +
                            // this.querySnapshotInfo + "]");
                        }
                        persistQuerySnapshotId(minSnapshotInfo);
                        this.querySnapshotInfo = minSnapshotInfo;
                        logger.debug("querySnapshotInfo updated to [" + minSnapshotInfo + "]");
                    } catch (IOException e) {
                        logger.error("update querySnapshotInfo failed", e);
                        return;
                    }
                    long newSnapshotId = minSnapshotInfo.getSnapshotId();
                    long newDdlSnapshotId = minSnapshotInfo.getDdlSnapshotId();
                    NavigableMap<Long, List<SnapshotListener>> listenersToTrigger =
                            this.snapshotToListeners.headMap(newSnapshotId, true);
                    for (Map.Entry<Long, List<SnapshotListener>> listenerEntry :
                            listenersToTrigger.entrySet()) {
                        List<SnapshotListener> listeners = listenerEntry.getValue();
                        for (SnapshotListener listener : listeners) {
                            try {
                                listener.onSnapshotAvailable();
                            } catch (Exception e) {
                                logger.warn(
                                        "trigger snapshotListener failed. snapshotId ["
                                                + snapshotId
                                                + "]");
                            }
                        }
                    }
                    listenersToTrigger.clear();

                    for (QuerySnapshotListener listener : this.listeners) {
                        try {
                            listener.snapshotAdvanced(newSnapshotId, newDdlSnapshotId);
                        } catch (Exception e) {
                            logger.error("error occurred when notify normal listeners", e);
                        }
                    }
                }
            }
        }
    }

    private void persistQuerySnapshotId(SnapshotInfo snapshotInfo) throws IOException {
        byte[] b = this.objectMapper.writeValueAsBytes(snapshotInfo);
        this.metaStore.write(QUERY_SNAPSHOT_INFO_PATH, b);
    }

    public long increaseWriteSnapshotId() throws IOException {
        this.writeSnapshotLock.lock();
        try {
            long snapshotId = this.writeSnapshotId + 1;
            persistWriteSnapshotId(snapshotId);
            this.writeSnapshotId = snapshotId;
            this.writeSnapshotIdNotifier.notifyWriteSnapshotIdChanged(this.writeSnapshotId);
            return this.writeSnapshotId;
        } finally {
            this.writeSnapshotLock.unlock();
        }
    }

    public void lockWriteSnapshot() {
        this.writeSnapshotLock.lock();
    }

    public void unlockWriteSnapshot() {
        this.writeSnapshotLock.unlock();
    }

    public long getCurrentWriteSnapshotId() {
        return this.writeSnapshotId;
    }

    private void persistWriteSnapshotId(long snapshotId) throws IOException {
        byte[] b = this.objectMapper.writeValueAsBytes(snapshotId);
        this.metaStore.write(WRITE_SNAPSHOT_ID_PATH, b);
    }

    private void updateQueueOffsets() throws IOException {
        if (this.storeToOffsets.size() < this.storeCount) {
            logger.warn(
                    "Not all store nodes reported queue offsets. current storeToOffsets ["
                            + this.storeToOffsets
                            + "]");
            return;
        }
        List<Long> queueOffsets = this.queueOffsetsRef.get();
        List<Long> newQueueOffsets = new ArrayList<>(queueOffsets);
        boolean changed = false;
        for (int qId = 0; qId < queueOffsets.size(); qId++) {
            long minOffset = Long.MAX_VALUE;
            for (List<Long> storeOffsets : this.storeToOffsets.values()) {
                minOffset = Math.min(storeOffsets.get(qId), minOffset);
            }
            if (minOffset != Long.MAX_VALUE && minOffset > newQueueOffsets.get(qId)) {
                newQueueOffsets.set(qId, minOffset);
                changed = true;
            }
        }
        if (changed) {
            persistQueueOffsets(newQueueOffsets);
            this.queueOffsetsRef.set(newQueueOffsets);
        }
    }

    private void persistQueueOffsets(List<Long> queueOffsets) throws IOException {
        byte[] bytes = this.objectMapper.writeValueAsBytes(queueOffsets);
        this.metaStore.write(QUEUE_OFFSETS_PATH, bytes);
    }

    public SnapshotInfo getQuerySnapshotInfo() {
        synchronized (this.querySnapshotLock) {
            return querySnapshotInfo;
        }
    }

    /**
     * Get offset list according to the input queueId list. This is for IngestNode to get the
     * correct start offset for replay.
     *
     * @param queueIdList
     * @return
     */
    public List<Long> getTailOffsets(List<Integer> queueIdList) {
        List<Long> tailOffsets = new ArrayList<>(queueIdList.size());
        List<Long> queueOffsets = this.queueOffsetsRef.get();
        for (int queueId : queueIdList) {
            tailOffsets.add(queueOffsets.get(queueId));
        }
        return tailOffsets;
    }

    public List<Long> getQueueOffsets() {
        return this.queueOffsetsRef.get();
    }
}
