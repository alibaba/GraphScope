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
package com.alibaba.graphscope.groot.store;

import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.operation.StoreDataBatch;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.groot.coordinator.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * WriterAgent is running on the GraphNode, it will cache data from IngestNode and write to the
 * store engine in the order of (snapshotId, queueId). WriterAgent will also send the ingest
 * progress to the SnapshotManager.
 */
public class WriterAgent {
    private static final Logger logger = LoggerFactory.getLogger(WriterAgent.class);

    private Configs configs;
    private int storeId;
    private int queueCount;
    private StoreService storeService;
    private MetaService metaService;
    private SnapshotCommitter snapshotCommitter;

    private volatile boolean shouldStop = true;
    private SnapshotSortQueue bufferQueue;
    private volatile long lastCommitSnapshotId;
    private volatile long consumeSnapshotId;
    private volatile long consumeDdlSnapshotId;
    private AtomicReference<SnapshotInfo> availSnapshotInfoRef;
    private ExecutorService commitExecutor;
    private List<Long> consumedQueueOffsets;
    private Thread consumeThread;

    public WriterAgent(
            Configs configs,
            StoreService storeService,
            MetaService metaService,
            SnapshotCommitter snapshotCommitter) {
        this.configs = configs;
        this.storeId = CommonConfig.NODE_IDX.get(configs);
        this.queueCount = metaService.getQueueCount();
        this.storeService = storeService;
        this.metaService = metaService;
        this.snapshotCommitter = snapshotCommitter;
        this.availSnapshotInfoRef = new AtomicReference<>();
    }

    /** should be called once, before start */
    public void init(long availSnapshotId) {
        this.availSnapshotInfoRef.set(new SnapshotInfo(availSnapshotId, availSnapshotId));
    }

    public void start() {
        this.lastCommitSnapshotId = -1L;
        this.consumeSnapshotId = 0L;
        this.consumeDdlSnapshotId = 0L;

        this.shouldStop = false;
        this.bufferQueue = new SnapshotSortQueue(this.configs, this.metaService);
        this.consumedQueueOffsets = new ArrayList<>(this.queueCount);
        for (int i = 0; i < this.queueCount; i++) {
            this.consumedQueueOffsets.add(-1L);
        }

        this.consumeThread = new Thread(() -> processBatches());
        this.consumeThread.setName("store-consume");
        this.consumeThread.setDaemon(true);
        this.consumeThread.start();

        this.commitExecutor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "writer-agent-commit", logger));
        logger.info("WriterAgent started");
    }

    public void stop() {
        this.shouldStop = true;
        if (this.consumeThread != null) {
            this.consumeThread.interrupt();
            try {
                this.consumeThread.join(3000);
            } catch (InterruptedException e) {
                // Do nothing
            }
            this.consumeThread = null;
        }
        if (this.commitExecutor != null) {
            this.commitExecutor.shutdown();
            try {
                this.commitExecutor.awaitTermination(3000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Do nothing
            }
            this.commitExecutor = null;
        }
        logger.info("WriterAgent stopped");
    }

    /**
     * Write data to store engine. This method will return immediately when the data is written to
     * the local buffer.
     *
     * @param storeDataBatch
     * @return True if offer success, otherwise False
     */
    public boolean writeStore(StoreDataBatch storeDataBatch) throws InterruptedException {
        int queueId = storeDataBatch.getQueueId();
        boolean suc = this.bufferQueue.offerQueue(queueId, storeDataBatch);
        return suc;
    }

    private void processBatches() {
        while (!shouldStop) {
            try {
                StoreDataBatch storeDataBatch = this.bufferQueue.poll();
                if (storeDataBatch == null) {
                    continue;
                }
                long batchSnapshotId = storeDataBatch.getSnapshotId();
                logger.debug("polled one batch [" + batchSnapshotId + "]");
                boolean hasDdl = writeEngineWithRetry(storeDataBatch);
                if (this.consumeSnapshotId < batchSnapshotId) {
                    SnapshotInfo availSnapshotInfo = this.availSnapshotInfoRef.get();
                    long availDdlSnapshotId = availSnapshotInfo.getDdlSnapshotId();
                    if (availDdlSnapshotId < this.consumeDdlSnapshotId) {
                        availDdlSnapshotId = this.consumeDdlSnapshotId;
                    }
                    long prevSnapshotId = batchSnapshotId - 1;
                    long availSnapshotId = availSnapshotInfo.getSnapshotId();
                    if (availSnapshotId < prevSnapshotId) {
                        availSnapshotId = prevSnapshotId;
                    }
                    this.consumeSnapshotId = batchSnapshotId;
                    this.availSnapshotInfoRef.set(
                            new SnapshotInfo(availSnapshotId, availDdlSnapshotId));
                    this.commitExecutor.execute(() -> asyncCommit());
                }

                if (hasDdl) {
                    this.consumeDdlSnapshotId = batchSnapshotId;
                }

                int queueId = storeDataBatch.getQueueId();
                long offset = storeDataBatch.getOffset();
                this.consumedQueueOffsets.set(queueId, offset);
            } catch (Exception e) {
                logger.error("error in processBatches, ignore", e);
            }
        }
    }

    private void asyncCommit() {
        SnapshotInfo snapshotInfo = this.availSnapshotInfoRef.get();
        long availSnapshotId = snapshotInfo.getSnapshotId();
        if (availSnapshotId > this.lastCommitSnapshotId) {
            long ddlSnapshotId = snapshotInfo.getDdlSnapshotId();
            List<Long> queueOffsets = new ArrayList<>(this.consumedQueueOffsets);
            try {
                logger.debug(
                        "commit snapshotId ["
                                + availSnapshotId
                                + "], last DDL snapshotId ["
                                + ddlSnapshotId
                                + "]");
                this.snapshotCommitter.commitSnapshotId(
                        this.storeId, availSnapshotId, ddlSnapshotId, queueOffsets);
                this.lastCommitSnapshotId = availSnapshotId;
            } catch (Exception e) {
                logger.warn(
                        "commit failed. snapshotId ["
                                + availSnapshotId
                                + "], queueOffsets ["
                                + queueOffsets
                                + "]. will ignore",
                        e);
            }
        }
    }

    private boolean writeEngineWithRetry(StoreDataBatch storeDataBatch) {
        while (!shouldStop) {
            try {
                return this.storeService.batchWrite(storeDataBatch);
            } catch (Exception e) {
                logger.error(
                        "writeEngine failed. queueId ["
                                + storeDataBatch.getQueueId()
                                + "], "
                                + "snapshotId ["
                                + storeDataBatch.getSnapshotId()
                                + "], "
                                + "offset ["
                                + storeDataBatch.getOffset()
                                + "]. will retry",
                        e);
            }
        }
        return false;
    }
}
