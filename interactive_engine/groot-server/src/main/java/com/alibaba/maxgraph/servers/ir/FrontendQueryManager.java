/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.maxgraph.servers.ir;

import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.groot.frontend.SnapshotUpdateCommitter;
import com.alibaba.maxgraph.common.util.CommonUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class FrontendQueryManager extends IrMetaQueryCallback {
    private static final Logger logger = LoggerFactory.getLogger(FrontendQueryManager.class);
    private static final int QUEUE_SIZE = 1024 * 1024;

    // manage queries <snapshotId, is_done>
    private BlockingQueue<QueryStatus> queryQueue;
    private SnapshotUpdateCommitter committer;
    private ScheduledExecutorService updateExecutor;
    private long oldSnapshotId = Long.MIN_VALUE;
    private int frontendId;

    public FrontendQueryManager(
            IrMetaFetcher fetcher, int frontendId, SnapshotUpdateCommitter committer) {
        super(fetcher);
        this.queryQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
        this.committer = committer;
        this.frontendId = frontendId;
    }

    public void start() {
        updateExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        CommonUtil.createFactoryWithDefaultExceptionHandler(
                                "groot-snapshot-manager", logger));
        updateExecutor.scheduleWithFixedDelay(
                new UpdateSnapshot(), 5000, 2000, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (updateExecutor != null) {
            updateExecutor.shutdownNow();
            try {
                if (!updateExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    logger.error("updateExecutor await timeout before shutdown");
                }
            } catch (InterruptedException e) {
                logger.error("updateExecutor awaitTermination exception ", e);
            }
        }
    }

    @Override
    public synchronized IrMeta beforeExec() {
        try {
            IrMeta irMeta = super.beforeExec();
            QueryStatus status = new QueryStatus(irMeta.getSnapshotId());
            queryQueue.put(status);
            return irMeta;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // set the QueryStatus as done after the execution of the query
    @Override
    public synchronized void afterExec(IrMeta irMeta) {
        long snapshotId = irMeta.getSnapshotId();
        queryQueue.forEach(
                k -> {
                    if (k.snapshotId == snapshotId) {
                        k.isDone = true;
                    }
                });
    }

    private class UpdateSnapshot implements Runnable {
        @Override
        public void run() {
            long minSnapshotId = 0L;
            logger.info("updateSnapshot start");
            try {
                while (!queryQueue.isEmpty() && queryQueue.peek().isDone) {
                    queryQueue.remove();
                }
                if (queryQueue.isEmpty()) {
                    minSnapshotId = fetcher.fetch().get().getSnapshotId();
                } else {
                    minSnapshotId = queryQueue.peek().snapshotId;
                }
                if (minSnapshotId > oldSnapshotId) {
                    committer.updateSnapshot(frontendId, minSnapshotId);
                    logger.info("update minSnapshotId {} success", minSnapshotId);
                    oldSnapshotId = minSnapshotId;
                } else {
                    logger.info("no new snapshot to update");
                }
            } catch (Exception e) {
                logger.error("update minSnapshotId {} fail", minSnapshotId, e);
            }
        }
    }

    private class QueryStatus {
        public long snapshotId;
        public boolean isDone;

        public QueryStatus(long snapshotId) {
            this.snapshotId = snapshotId;
            this.isDone = false;
        }
    }
}
