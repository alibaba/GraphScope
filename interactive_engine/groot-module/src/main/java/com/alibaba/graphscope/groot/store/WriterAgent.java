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

import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.groot.coordinator.SnapshotInfo;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.operation.StoreDataBatch;
import com.alibaba.graphscope.groot.rpc.RoleClients;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * WriterAgent is running on the GraphNode, it will cache data from IngestNode and write to the
 * store engine in the order of (snapshotId, queueId). WriterAgent will also send the ingest
 * progress to the SnapshotManager.
 */
public class WriterAgent {
    private static final Logger logger = LoggerFactory.getLogger(WriterAgent.class);

    private final Configs configs;
    private final int storeId;
    private final int queueCount;
    private final StoreService storeService;
    private final MetaService metaService;
    private final RoleClients<SnapshotCommitClient> snapshotCommitter;

    private volatile boolean shouldStop = true;
    private SnapshotSortQueue bufferQueue;
    private volatile long lastCommitSI;
    private volatile long consumeSI;
    private volatile long consumeDdlSnapshotId;
    private final AtomicReference<SnapshotInfo> availSnapshotInfoRef;
    private ExecutorService commitExecutor;
    private List<Long> consumedQueueOffsets;
    private Thread consumeThread;

    public WriterAgent(
            Configs configs,
            StoreService storeService,
            MetaService metaService,
            RoleClients<SnapshotCommitClient> snapshotCommitter) {
        this.configs = configs;
        this.storeId = CommonConfig.NODE_IDX.get(configs);
        this.queueCount = metaService.getQueueCount();
        this.storeService = storeService;
        this.metaService = metaService;
        this.snapshotCommitter = snapshotCommitter;
        this.availSnapshotInfoRef = new AtomicReference<>();
        initMetrics();
    }

    public void start() {
        this.lastCommitSI = -1L;
        this.consumeSI = 0L;
        this.consumeDdlSnapshotId = 0L;
        this.availSnapshotInfoRef.set(new SnapshotInfo(0, 0));

        this.shouldStop = false;
        this.bufferQueue = new SnapshotSortQueue(this.configs, this.metaService);
        this.consumedQueueOffsets = new ArrayList<>(this.queueCount);
        for (int i = 0; i < this.queueCount; i++) {
            this.consumedQueueOffsets.add(-1L);
        }

        this.commitExecutor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "writer-agent-commit", logger));

        this.consumeThread = new Thread(this::processBatches);
        this.consumeThread.setName("store-consume");
        this.consumeThread.setDaemon(true);
        this.consumeThread.start();
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

        logger.debug("WriterAgent stopped");
    }

    /**
     * Write data to store engine. This method will return immediately when the data is written to
     * the local buffer.
     *
     * @param storeDataBatch
     * @return True if offer success, otherwise False
     */
    public boolean writeStore(StoreDataBatch storeDataBatch) throws InterruptedException {
        // logger.info("writeStore {}", storeDataBatch.toProto());
        //        int queueId = storeDataBatch.getQueueId();
        boolean suc = this.bufferQueue.offerQueue(0, storeDataBatch);
        //        logger.debug("Buffer queue: {}, {}", suc, this.bufferQueue.innerQueueSizes());
        return suc;
    }

    public boolean writeStore2(List<StoreDataBatch> storeDataBatches) throws InterruptedException {
        for (StoreDataBatch storeDataBatch : storeDataBatches) {
            int queueId = storeDataBatch.getQueueId();
            if (!this.bufferQueue.offerQueue(queueId, storeDataBatch)) {
                return false;
            }
        }
        return true;
    }

    private void processBatches() {
        while (!shouldStop) {
            try {
                StoreDataBatch batch = this.bufferQueue.poll();
                if (batch == null) {
                    continue;
                }
                long batchSI = batch.getSnapshotId();
                logger.debug("polled one batch [" + batchSI + "]");
                boolean hasDdl = writeEngineWithRetry(batch);
                if (this.consumeSI < batchSI) {
                    SnapshotInfo availSInfo = this.availSnapshotInfoRef.get();
                    long availSI = Math.max(availSInfo.getSnapshotId(), batchSI - 1);
                    long availDdlSI = Math.max(availSInfo.getDdlSnapshotId(), consumeDdlSnapshotId);
                    this.consumeSI = batchSI;
                    this.availSnapshotInfoRef.set(new SnapshotInfo(availSI, availDdlSI));
                    this.commitExecutor.execute(this::asyncCommit);
                } else { // a flurry of batches with same snapshot ID
                    logger.debug("consumedSI {} >= batchSI {}, ignored", consumeSI, batchSI);
                }
                if (hasDdl) {
                    this.consumeDdlSnapshotId = batchSI;
                }
                // this.consumedQueueOffsets.set(batch.getQueueId(), batch.getOffset());
                this.consumedQueueOffsets.set(0, batch.getOffset());
            } catch (InterruptedException e) {
                logger.error("processBatches interrupted");
            } catch (Exception e) {
                logger.error("error in processBatches, ignore", e);
            }
        }
    }

    private void asyncCommit() {
        SnapshotInfo snapshotInfo = this.availSnapshotInfoRef.get();
        long curSI = snapshotInfo.getSnapshotId();
        if (curSI > this.lastCommitSI) {
            long ddlSnapshotId = snapshotInfo.getDdlSnapshotId();
            List<Long> queueOffsets = new ArrayList<>(this.consumedQueueOffsets);
            try {
                // logger.info("commit SI {}, last DDL SI {}", availSnapshotId, ddlSnapshotId);
                this.snapshotCommitter
                        .getClient(0)
                        .commitSnapshotId(storeId, curSI, ddlSnapshotId, queueOffsets);
                this.lastCommitSI = curSI;
            } catch (Exception e) {
                logger.warn("commit failed. SI {}, offset {}. ignored", curSI, queueOffsets, e);
            }
        } else {
            logger.warn("curSI {} <= lastCommitSI {}, ignored", curSI, lastCommitSI);
        }
    }

    private boolean writeEngineWithRetry(StoreDataBatch storeDataBatch) {
        while (!shouldStop) {
            try {
                return this.storeService.batchWrite(storeDataBatch);
            } catch (Exception e) {
                logger.error("writeEngine failed: batch {}.", storeDataBatch.toProto(), e);
            }
        }
        return false;
    }

    public List<Long> getConsumedQueueOffsets() {
        return consumedQueueOffsets;
    }

    public void initMetrics() {
        Meter meter = GlobalOpenTelemetry.getMeter("default");
        meter.upDownCounterBuilder("groot.store.writer.queue.size")
                .setDescription("The buffer queue size of writer agent within the store node.")
                .buildWithCallback(measurement -> measurement.record(bufferQueue.size()));
    }
}
