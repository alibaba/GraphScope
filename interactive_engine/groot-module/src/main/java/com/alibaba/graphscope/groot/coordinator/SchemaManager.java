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

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.common.exception.ServiceNotReadyException;
import com.alibaba.graphscope.groot.common.schema.wrapper.GraphDef;
import com.alibaba.graphscope.groot.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.operation.BatchId;
import com.alibaba.graphscope.groot.operation.Operation;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.schema.ddl.DdlExecutors;
import com.alibaba.graphscope.groot.schema.ddl.DdlResult;
import com.alibaba.graphscope.groot.schema.request.DdlRequestBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);

    private final SnapshotManager snapshotManager;
    private final DdlWriter ddlWriter;
    private final DdlExecutors ddlExecutors;
    private final GraphDefFetcher graphDefFetcher;

    private final AtomicReference<GraphDef> graphDefRef;
    private final int partitionCount;
    private volatile boolean ready = false;

    private ExecutorService singleThreadExecutor;
    private ScheduledExecutorService scheduler;

    public SchemaManager(
            SnapshotManager snapshotManager,
            DdlExecutors ddlExecutors,
            DdlWriter ddlWriter,
            MetaService metaService,
            GraphDefFetcher graphDefFetcher) {
        this.snapshotManager = snapshotManager;
        this.ddlExecutors = ddlExecutors;
        this.ddlWriter = ddlWriter;
        this.graphDefFetcher = graphDefFetcher;

        this.graphDefRef = new AtomicReference<>();
        this.partitionCount = metaService.getPartitionCount();
    }

    public void start() {
        logger.info("starting SchemaManager...");
        this.singleThreadExecutor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "ddl-executor", logger));
        recover();
        logger.info(graphDefRef.get().toProto().toString());

        this.scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "recover", logger));
        this.scheduler.scheduleWithFixedDelay(this::recover, 5, 2, TimeUnit.SECONDS);
    }

    private void recover() {
        try {
            recoverInternal();
        } catch (Exception e) {
            logger.error("recover schemaManager failed", e);
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException interruptedException) {
                // Ignore
            }
            this.singleThreadExecutor.execute(this::recover);
        }
    }

    private void recoverInternal() throws IOException, ExecutionException, InterruptedException {
        long snapshotId = this.snapshotManager.increaseWriteSnapshotId();
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.snapshotManager.addSnapshotListener(snapshotId, () -> future.complete(null));
        future.get();
        GraphDef graphDef = this.graphDefFetcher.fetchGraphDef();
        this.graphDefRef.set(graphDef);
        this.ready = true;
        logger.debug("SchemaManager recovered. version [" + graphDef.getVersion() + "]");
    }

    public void stop() {
        if (this.singleThreadExecutor != null) {
            this.singleThreadExecutor.shutdown();
            try {
                this.singleThreadExecutor.awaitTermination(3000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
    }

    public void submitBatchDdl(
            String requestId,
            String sessionId,
            DdlRequestBatch ddlRequestBatch,
            CompletionCallback<Long> callback) {
        logger.info(
                "submitBatchDdl requestId [{}], sessionId [{}], request body [{}]",
                requestId,
                sessionId,
                ddlRequestBatch.toProto());
        if (!ready) {
            callback.onError(new IllegalStateException("SchemaManager is recovering"));
            return;
        }
        this.singleThreadExecutor.execute(
                () -> {
                    try {
                        if (!ready) {
                            callback.onError(
                                    new IllegalStateException("SchemaManager is recovering"));
                            return;
                        }
                        GraphDef tmpGraphDef = this.graphDefRef.get();
                        DdlResult ddlResult =
                                this.ddlExecutors.executeDdlRequestBatch(
                                        ddlRequestBatch, tmpGraphDef, this.partitionCount);
                        GraphDef graphDefResult = ddlResult.getGraphDef();
                        List<Operation> ddlOperations = ddlResult.getDdlOperations();
                        this.snapshotManager.lockWriteSnapshot();
                        BatchId batchId;
                        try {
                            long currentWriteSnapshotId =
                                    this.snapshotManager.getCurrentWriteSnapshotId();
                            OperationBatch operationBatch =
                                    OperationBatch.newBuilder(ddlOperations)
                                            .setLatestSnapshotId(currentWriteSnapshotId)
                                            .build();
                            batchId = this.ddlWriter.writeOperations(requestId, operationBatch);
                        } finally {
                            this.snapshotManager.unlockWriteSnapshot();
                        }
                        long snapshotId = batchId.getSnapshotId();
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        this.snapshotManager.addSnapshotListener(
                                snapshotId,
                                () -> {
                                    this.graphDefRef.set(graphDefResult);
                                    future.complete(null);
                                });
                        future.get();
                        callback.onCompleted(snapshotId);
                    } catch (Exception e) {
                        logger.error(
                                "Error in Ddl requestId [{}], sessionId [{}]",
                                requestId,
                                sessionId,
                                e);
                        this.ready = false;
                        callback.onError(e);
                        this.singleThreadExecutor.execute(() -> recover());
                    }
                });
    }

    public GraphDef getGraphDef() {
        if (!ready) {
            throw new ServiceNotReadyException("SchemaManager is recovering");
        }
        return this.graphDefRef.get();
    }
}
