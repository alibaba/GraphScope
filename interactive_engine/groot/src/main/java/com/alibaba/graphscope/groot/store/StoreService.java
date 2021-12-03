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

import com.alibaba.maxgraph.proto.groot.GraphDefPb;
import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.StoreDataBatch;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.StoreConfig;
import com.alibaba.maxgraph.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.groot.store.jna.JnaGraphStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StoreService {
    private static final Logger logger = LoggerFactory.getLogger(StoreService.class);

    private Configs configs;
    private int storeId;
    private int writeThreadCount;
    private MetaService metaService;
    private Map<Integer, GraphPartition> idToPartition;
    private ExecutorService writeExecutor, ingestExecutor;
    private volatile boolean shouldStop = true;

    public StoreService(Configs configs, MetaService metaService) {
        this.configs = configs;
        this.storeId = CommonConfig.NODE_IDX.get(configs);
        this.writeThreadCount = StoreConfig.STORE_WRITE_THREAD_COUNT.get(configs);
        this.metaService = metaService;
    }

    public void start() throws IOException {
        logger.info("starting StoreService...");
        this.shouldStop = false;
        this.writeExecutor =
                new ThreadPoolExecutor(
                        writeThreadCount,
                        writeThreadCount,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "store-write", logger));
        this.ingestExecutor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "store-ingest", logger));
        List<Integer> partitionIds = this.metaService.getPartitionsByStoreId(this.storeId);
        this.idToPartition = new HashMap<>(partitionIds.size());
        for (int partitionId : partitionIds) {
            GraphPartition partition = makeGraphPartition(this.configs, partitionId);
            this.idToPartition.put(partitionId, partition);
        }
        logger.info("StoreService started. storeId [" + this.storeId + "]");
    }

    public GraphPartition makeGraphPartition(Configs configs, int partitionId) throws IOException {
        return new JnaGraphStore(configs, partitionId);
    }

    public Map<Integer, GraphPartition> getIdToPartition() {
        return this.idToPartition;
    }

    public void stop() {
        this.shouldStop = true;
        if (this.idToPartition != null) {
            CountDownLatch latch = new CountDownLatch(this.idToPartition.size());
            for (GraphPartition partition : this.idToPartition.values()) {
                this.writeExecutor.execute(
                        () -> {
                            try {
                                partition.close();
                                logger.info("partition #[" + partition.getId() + "] closed");
                            } catch (IOException e) {
                                logger.error(
                                        "partition #[" + partition.getId() + "] close failed", e);
                            } finally {
                                latch.countDown();
                            }
                        });
            }
            try {
                long waitSeconds = 30L;
                if (!latch.await(waitSeconds, TimeUnit.SECONDS)) {
                    logger.warn("not all partitions closed, waited [" + waitSeconds + "] seconds");
                }
            } catch (InterruptedException e) {
                // Ignore
            }
            this.idToPartition = null;
        }
        if (this.writeExecutor != null) {
            this.writeExecutor.shutdown();
            try {
                this.writeExecutor.awaitTermination(3000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
            this.writeExecutor = null;
        }
    }

    /**
     * recover data from disk
     *
     * @return snapshotId of recovered data.
     */
    public long recover() throws InterruptedException, IOException {
        AtomicLong snapshotId = new AtomicLong(Long.MAX_VALUE);
        CountDownLatch latch = new CountDownLatch(this.idToPartition.size());
        for (GraphPartition partition : this.idToPartition.values()) {
            this.writeExecutor.execute(
                    () -> {
                        try {
                            long partitionSnapshotId = partition.recover();
                            snapshotId.updateAndGet(
                                    x -> x < partitionSnapshotId ? x : partitionSnapshotId);
                        } catch (Exception e) {
                            logger.error("partition #[] recover failed");
                            snapshotId.set(-1L);
                        } finally {
                            latch.countDown();
                        }
                    });
        }
        latch.await();
        long recoveredSnapshotId = snapshotId.get();
        if (recoveredSnapshotId == -1L) {
            throw new IOException("recover data failed");
        }
        logger.info("store data recovered, snapshotId [" + recoveredSnapshotId + "]");
        return recoveredSnapshotId;
    }

    public boolean batchWrite(StoreDataBatch storeDataBatch)
            throws ExecutionException, InterruptedException {
        long snapshotId = storeDataBatch.getSnapshotId();
        List<Map<Integer, OperationBatch>> dataBatch = storeDataBatch.getDataBatch();
        AtomicBoolean hasDdl = new AtomicBoolean(false);
        for (Map<Integer, OperationBatch> partitionToBatch : dataBatch) {
            while (!shouldStop && partitionToBatch.size() != 0) {
                partitionToBatch = writeStore(snapshotId, partitionToBatch, hasDdl);
            }
        }
        return hasDdl.get();
    }

    private Map<Integer, OperationBatch> writeStore(
            long snapshotId, Map<Integer, OperationBatch> partitionToBatch, AtomicBoolean hasDdl)
            throws ExecutionException, InterruptedException {
        Map<Integer, OperationBatch> batchNeedRetry = new ConcurrentHashMap<>();
        AtomicInteger counter = new AtomicInteger(partitionToBatch.size());
        CompletableFuture<Object> future = new CompletableFuture<>();
        for (Map.Entry<Integer, OperationBatch> e : partitionToBatch.entrySet()) {
            int partitionId = e.getKey();
            OperationBatch batch = e.getValue();
            logger.debug("writeStore partition [" + partitionId + "]");
            this.writeExecutor.execute(
                    () -> {
                        try {
                            if (partitionId != -1) {
                                // Ignore Marker
                                // Only support partition operation for now
                                GraphPartition partition = this.idToPartition.get(partitionId);
                                if (partition == null) {
                                    throw new IllegalStateException(
                                            "partition ["
                                                    + partitionId
                                                    + "] is not initialized / exists");
                                }
                                if (partition.writeBatch(snapshotId, batch)) {
                                    hasDdl.set(true);
                                }
                            }
                        } catch (Exception ex) {
                            logger.error(
                                    "write to partition ["
                                            + partitionId
                                            + "] failed, snapshotId ["
                                            + snapshotId
                                            + "]. will retry",
                                    ex);
                            batchNeedRetry.put(partitionId, batch);
                        }
                        if (counter.decrementAndGet() == 0) {
                            future.complete(null);
                        }
                    });
        }
        future.get();
        if (batchNeedRetry.size() > 0) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
        return batchNeedRetry;
    }

    public GraphDefPb getGraphDefBlob() throws IOException {
        GraphPartition graphPartition = this.idToPartition.get(0);
        return graphPartition.getGraphDefBlob();
    }

    public MetaService getMetaService() {
        return this.metaService;
    }

    public void ingestData(String path, CompletionCallback<Void> callback) {
        this.ingestExecutor.execute(
                () -> {
                    try {
                        logger.info("ingest data [" + path + "]");
                        ingestDataInternal(path);
                        callback.onCompleted(null);
                    } catch (Exception e) {
                        logger.error("ingest data failed. path [" + path + "]", e);
                        callback.onError(e);
                    }
                });
    }

    private void ingestDataInternal(String path) throws IOException {
        Path dataDir = new Path(path);
        Configuration conf = new Configuration();
        FileSystem fs = dataDir.getFileSystem(conf);
        for (Map.Entry<Integer, GraphPartition> entry : this.idToPartition.entrySet()) {
            int pid = entry.getKey();
            GraphPartition partition = entry.getValue();
            String fileName = "part-r-" + String.format("%05d", pid) + ".sst";
            Path realPath = new Path(dataDir, fileName);
            partition.ingestHdfsFile(fs, realPath);
        }
    }
}
