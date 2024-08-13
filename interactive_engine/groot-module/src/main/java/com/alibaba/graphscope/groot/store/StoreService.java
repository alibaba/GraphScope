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

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.StoreConfig;
import com.alibaba.graphscope.groot.common.exception.GrootException;
import com.alibaba.graphscope.groot.common.exception.IllegalStateException;
import com.alibaba.graphscope.groot.common.exception.InternalException;
import com.alibaba.graphscope.groot.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.groot.common.util.Utils;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.StoreDataBatch;
import com.alibaba.graphscope.groot.store.external.ExternalStorage;
import com.alibaba.graphscope.groot.store.jna.JnaGraphStore;
import com.alibaba.graphscope.proto.groot.GraphDefPb;
import com.alibaba.graphscope.proto.groot.Statistics;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.*;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

public class StoreService {
    private static final Logger logger = LoggerFactory.getLogger(StoreService.class);
    private static final Logger metricLogger = LoggerFactory.getLogger("MetricLog");

    private final Configs storeConfigs;
    private final int storeId;
    private final int writeThreadCount;
    private final int compactThreadCount;
    private final MetaService metaService;
    private Map<Integer, GraphPartition> idToPartition;
    private ExecutorService writeExecutor;
    private ExecutorService ingestExecutor;
    private ExecutorService garbageCollectExecutor;
    private ExecutorService compactExecutor;
    private ExecutorService statisticsExecutor;

    private ThreadPoolExecutor downloadExecutor;
    private final boolean enableGc;
    private volatile boolean shouldStop = true;
    private final boolean isSecondary;
    private LongCounter writeCounter;
    private LongHistogram writeHistogram;
    private LongHistogram gcHistogram;

    public StoreService(Configs storeConfigs, MetaService metaService) {
        this.storeConfigs = storeConfigs;
        this.storeId = CommonConfig.NODE_IDX.get(storeConfigs);
        this.enableGc = StoreConfig.STORE_GC_ENABLE.get(storeConfigs);
        this.writeThreadCount = StoreConfig.STORE_WRITE_THREAD_COUNT.get(storeConfigs);
        this.compactThreadCount = StoreConfig.STORE_COMPACT_THREAD_NUM.get(storeConfigs);
        this.metaService = metaService;
        this.isSecondary = CommonConfig.SECONDARY_INSTANCE_ENABLED.get(storeConfigs);
    }

    public void start() throws IOException {
        logger.info("starting StoreService...");
        List<Integer> partitionIds = this.metaService.getPartitionsByStoreId(this.storeId);
        this.idToPartition = new HashMap<>(partitionIds.size());
        for (int partitionId : partitionIds) {
            try {
                GraphPartition partition = makeGraphPartition(this.storeConfigs, partitionId);
                this.idToPartition.put(partitionId, partition);
            } catch (IOException e) {
                throw new InternalException(e);
            }
        }
        initMetrics();
        this.shouldStop = false;
        this.writeExecutor =
                new ThreadPoolExecutor(
                        writeThreadCount,
                        writeThreadCount,
                        0L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "store-write", logger));
        this.ingestExecutor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(1),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "store-ingest", logger));
        this.compactExecutor =
                new ThreadPoolExecutor(
                        1,
                        this.compactThreadCount,
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(partitionIds.size()),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "store-compact", logger));
        this.garbageCollectExecutor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "store-garbage-collect", logger));
        logger.info("StoreService started. storeId [" + this.storeId + "]");
        this.downloadExecutor =
                new ThreadPoolExecutor(
                        16,
                        16,
                        1L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "store-download", logger));
        this.downloadExecutor.allowCoreThreadTimeOut(true);
        this.statisticsExecutor =
                new ThreadPoolExecutor(
                        8,
                        16,
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "store-statistics", logger));
        logger.info("StoreService started. storeId [" + this.storeId + "]");
    }

    public GraphPartition makeGraphPartition(Configs configs, int partitionId) throws IOException {
        return new JnaGraphStore(configs, partitionId);
    }

    public Map<Integer, GraphPartition> getIdToPartition() {
        return this.idToPartition;
    }

    public int getStoreId() {
        return storeId;
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

    public boolean batchWrite(StoreDataBatch storeDataBatch)
            throws ExecutionException, InterruptedException {
        long snapshotId = storeDataBatch.getSnapshotId();
        List<Map<Integer, OperationBatch>> dataBatch = storeDataBatch.getDataBatch();
        AtomicBoolean hasDdl = new AtomicBoolean(false);
        int maxRetry = 10;
        for (Map<Integer, OperationBatch> partitionToBatch : dataBatch) {
            while (!shouldStop && partitionToBatch.size() != 0 && maxRetry > 0) {
                partitionToBatch = writeStore(snapshotId, partitionToBatch, hasDdl);
                maxRetry--;
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
            AttributesBuilder attrs = Attributes.builder().put("partition.id", partitionId);
            this.writeExecutor.execute(
                    () -> {
                        long start = System.currentTimeMillis();
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
                                metricLogger.info(
                                        buildMetricJsonLog(true, batch, start, partitionId));
                                attrs.put("success", true).put("message", "");
                                this.writeHistogram.record(
                                        System.currentTimeMillis() - start, attrs.build());
                                this.writeCounter.add(batch.getOperationCount(), attrs.build());
                            } //  else {
                            //     logger.debug("marker batch ignored");
                            // }
                        } catch (Exception ex) {
                            metricLogger.info(buildMetricJsonLog(false, batch, start, partitionId));
                            logger.error(
                                    "write to partition [{}] failed, snapshotId [{}], traceId"
                                            + " [{}].",
                                    partitionId,
                                    snapshotId,
                                    batch.getTraceId(),
                                    ex);
                            attrs.put("message", ex.getMessage());
                            String msg = "Not supported operation in secondary mode";
                            if (ex.getMessage().contains(msg)) {
                                logger.warn("Ignored write in secondary instance, {}", msg);
                                attrs.put("success", true);
                            } else {
                                attrs.put("success", false);
                                this.writeCounter.add(batch.getOperationCount(), attrs.build());
                                batchNeedRetry.put(partitionId, batch);
                            }
                            this.writeHistogram.record(
                                    System.currentTimeMillis() - start, attrs.build());
                        }
                        if (counter.decrementAndGet() == 0) {
                            future.complete(null);
                        }
                    });
        }
        future.get();
        if (batchNeedRetry.size() > 0) {
            logger.warn("Write batch failed, will retry. failure count: {}", batchNeedRetry.size());
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
        return batchNeedRetry;
    }

    private String buildMetricJsonLog(
            boolean succeed, OperationBatch operationBatch, long start, int partitionId) {
        String traceId = operationBatch.getTraceId();
        long current = System.currentTimeMillis();
        int batchSize = operationBatch.getOperationCount();
        return Utils.buildMetricJsonLog(
                succeed,
                traceId,
                batchSize,
                partitionId,
                (current - start),
                current,
                "writeDb",
                "write");
    }

    public GraphDefPb getGraphDefBlob() throws IOException {
        GraphPartition graphPartition = this.idToPartition.get(0);
        return graphPartition.getGraphDefBlob();
    }

    public Map<Integer, Statistics> getGraphStatisticsBlob(long snapshotId) throws IOException {
        int partitionCount = this.idToPartition.values().size();
        CountDownLatch countDownLatch = new CountDownLatch(partitionCount);
        logger.info("Collect statistics of store#{} started", storeId);
        Map<Integer, Statistics> statisticsMap = new ConcurrentHashMap<>();
        for (Map.Entry<Integer, GraphPartition> entry : idToPartition.entrySet()) {
            this.statisticsExecutor.execute(
                    () -> {
                        try {
                            Statistics statistics =
                                    entry.getValue().getGraphStatisticsBlob(snapshotId);
                            statisticsMap.put(entry.getKey(), statistics);
                            logger.debug("Collected statistics of partition#{}", entry.getKey());
                        } catch (IOException e) {
                            logger.error(
                                    "Collect statistics failed for partition {}",
                                    entry.getKey(),
                                    e);
                        } finally {
                            countDownLatch.countDown();
                        }
                    });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("collect statistics has been interrupted", e);
        }
        if (statisticsMap.size() != partitionCount) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
        logger.info("Collect statistics of store#{} done, size: {}", storeId, statisticsMap.size());
        return statisticsMap;
    }

    public MetaService getMetaService() {
        return this.metaService;
    }

    public void ingestData(
            String path, Map<String, String> config, CompletionCallback<Void> callback) {
        String dataRoot = StoreConfig.STORE_DATA_PATH.get(storeConfigs);
        String downloadPath = StoreConfig.STORE_DATA_DOWNLOAD_PATH.get(storeConfigs);
        if (downloadPath.isEmpty()) {
            downloadPath = Paths.get(dataRoot, "download").toString();
        }
        String[] items = path.split("/");
        // Get the unique path (uuid)
        String unique_path = items[items.length - 1];
        Path uniquePath = Paths.get(downloadPath, unique_path);
        if (!Files.isDirectory(uniquePath)) {
            try {
                Files.createDirectories(uniquePath);
                logger.info("Created uniquePath {}", uniquePath);
            } catch (IOException e) {
                logger.error("create uniquePath failed. uniquePath {}", uniquePath, e);
                callback.onError(e);
                return;
            }
        }
        this.ingestExecutor.execute(
                () -> {
                    logger.info("ingesting data [{}]", path);
                    try {
                        ingestDataInternal(path, config, callback);
                    } catch (Exception e) {
                        logger.error("ingest data failed. path [" + path + "]", e);
                        callback.onError(e);
                    }
                    logger.info("ingest data [{}] complete", path);
                });
    }

    private void ingestDataInternal(
            String path, Map<String, String> config, CompletionCallback<Void> callback)
            throws IOException {
        ExternalStorage externalStorage = ExternalStorage.getStorage(path, config);
        Set<Map.Entry<Integer, GraphPartition>> entries = this.idToPartition.entrySet();
        AtomicInteger counter = new AtomicInteger(entries.size());
        AtomicBoolean finished = new AtomicBoolean(false);
        for (Map.Entry<Integer, GraphPartition> entry : entries) {
            int pid = entry.getKey();
            GraphPartition partition = entry.getValue();
            String fileName = "part-r-" + String.format("%05d", pid) + ".sst";
            String fullPath = path + "/" + fileName;
            logger.info("downloading {}", fullPath);
            this.downloadExecutor.execute(
                    () -> {
                        try {
                            partition.ingestExternalFile(externalStorage, fullPath);
                        } catch (Exception e) {
                            logger.error("ingest external file failed.", e);
                            if (!finished.getAndSet(true)) {
                                callback.onError(e);
                            }
                        }
                        if (counter.decrementAndGet() == 0) {
                            logger.info("All download tasks finished.");
                            finished.set(true);
                            callback.onCompleted(null);
                        } else {
                            logger.info(counter.get() + " download tasks remaining");
                        }
                    });
        }
    }

    public void clearIngest(String uniquePath) throws IOException {
        if (uniquePath == null || uniquePath.isEmpty()) {
            logger.warn("Must set a sub-path for clearing.");
            return;
        }
        String dataRoot = StoreConfig.STORE_DATA_PATH.get(storeConfigs);
        String downloadPath = StoreConfig.STORE_DATA_DOWNLOAD_PATH.get(storeConfigs);
        if (downloadPath.isEmpty()) {
            downloadPath = Paths.get(dataRoot, "download").toString();
        }
        downloadPath = Paths.get(downloadPath, uniquePath).toString();
        try {
            logger.info("Clearing directory {}", downloadPath);
            FileUtils.forceDelete(new File(downloadPath));
        } catch (FileNotFoundException e) {
            // Ignore
        }
        logger.info("cleared directory {}", downloadPath);
    }

    public void garbageCollect(long snapshotId, CompletionCallback<Void> callback) {
        if (!enableGc) {
            callback.onError(new GrootException("store gc is not enabled"));
            return;
        }
        this.garbageCollectExecutor.execute(
                () -> {
                    try {
                        // logger.debug("Garbage collecting, snapshot [{}]", snapshotId);
                        garbageCollectInternal(snapshotId);
                        callback.onCompleted(null);
                    } catch (Exception e) {
                        logger.error("garbage collect failed. snapshot [{}]", snapshotId, e);
                        callback.onError(e);
                    }
                });
    }

    private void garbageCollectInternal(long snapshotId) throws IOException {
        for (Map.Entry<Integer, GraphPartition> entry : this.idToPartition.entrySet()) {
            Attributes attrs = Attributes.builder().put("partition.id", entry.getKey()).build();
            GraphPartition partition = entry.getValue();
            long start = System.currentTimeMillis();
            partition.garbageCollect(snapshotId);
            this.gcHistogram.record(System.currentTimeMillis() - start, attrs);
        }
    }

    public void compactDB(CompletionCallback<Void> callback) {
        if (isSecondary) {
            callback.onCompleted(null);
            return;
        }
        int partitionCount = this.idToPartition.values().size();
        CountDownLatch compactCountDownLatch = new CountDownLatch(partitionCount);
        AtomicInteger successCompactJobCount = new AtomicInteger(partitionCount);
        logger.info("compaction of all DB started");
        for (GraphPartition partition : this.idToPartition.values()) {
            this.compactExecutor.execute(
                    () -> {
                        try {
                            logger.info("Compaction of {} partition started", partition.getId());
                            partition.compact();
                            logger.info("Compaction of {} partition finished", partition.getId());
                            successCompactJobCount.decrementAndGet();
                        } catch (Exception e) {
                            logger.error("compact DB failed", e);
                        } finally {
                            compactCountDownLatch.countDown();
                        }
                    });
        }

        try {
            compactCountDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("compact DB has been InterruptedException", e);
        }

        if (successCompactJobCount.get() > 0) {
            String msg = "not all partition compact success. please check log.";
            logger.error(msg);
            callback.onError(new Exception(msg));
        } else {
            callback.onCompleted(null);
        }
    }

    public void tryCatchUpWithPrimary() throws IOException {
        if (!isSecondary) {
            return;
        }
        for (GraphPartition partition : this.idToPartition.values()) {
            partition.tryCatchUpWithPrimary();
        }
    }

    public void reopenPartition(long wait_sec, CompletionCallback<Void> callback) {
        if (!isSecondary) {
            callback.onCompleted(null);
            return;
        }
        try {
            for (GraphPartition partition : this.idToPartition.values()) {
                partition.reopenSecondary(wait_sec);
            }
            callback.onCompleted(null);
        } catch (Exception e) {
            logger.error("reopen secondary failed", e);
            callback.onError(e);
        }
    }

    public void initMetrics() {
        Meter meter = GlobalOpenTelemetry.getMeter("default");
        this.writeCounter =
                meter.counterBuilder("groot.store.write.count")
                        .setDescription("Total count of write requests of one store node.")
                        .build();
        this.writeHistogram =
                meter.histogramBuilder("groot.store.write.duration")
                        .setDescription("Duration of write requests that be persist into the disk.")
                        .ofLongs()
                        .setUnit("ms")
                        .build();
        this.gcHistogram =
                meter.histogramBuilder("groot.store.gc.duration")
                        .setDescription("Duration of the garbage collect process.")
                        .ofLongs()
                        .setUnit("ms")
                        .build();

        meter.upDownCounterBuilder("groot.store.disk.usage")
                .setDescription("Percentage of disk space in use.")
                .setUnit("percents")
                .ofDoubles()
                .buildWithCallback(
                        result -> {
                            long[] ret = getDiskStatus();
                            result.record(ret[0] * 1.0 / ret[1]);
                        });
    }

    public long[] getDiskStatus() {
        String dataRoot = StoreConfig.STORE_DATA_PATH.get(storeConfigs);
        File file = new File(dataRoot);
        long total = file.getTotalSpace();
        long usable = file.getUsableSpace();
        return new long[] {total, usable};
    }
}
