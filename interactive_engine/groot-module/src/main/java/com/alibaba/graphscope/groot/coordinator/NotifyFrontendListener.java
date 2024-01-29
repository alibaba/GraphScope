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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class NotifyFrontendListener implements QuerySnapshotListener {
    private static final Logger logger = LoggerFactory.getLogger(NotifyFrontendListener.class);

    private final int frontendId;
    private final FrontendSnapshotClient frontendSnapshotClient;
    private final SchemaManager schemaManager;

    private final AtomicLong lastDdlSnapshotId;

    public NotifyFrontendListener(
            int frontendId,
            FrontendSnapshotClient frontendSnapshotClient,
            SchemaManager schemaManager) {
        this.frontendId = frontendId;
        this.frontendSnapshotClient = frontendSnapshotClient;
        this.schemaManager = schemaManager;

        this.lastDdlSnapshotId = new AtomicLong(-1L);
    }

    @Override
    public void snapshotAdvanced(long snapshotId, long ddlSnapshotId) {
        GraphDef graphDef;
        try {
            graphDef = this.schemaManager.getGraphDef();
        } catch (ServiceNotReadyException ex) {
            logger.warn("Schema manager is not ready: {}", ex.getMessage());
            return;
        }
        logger.debug("snapshot advanced to {}-{}, will notify frontend", snapshotId, ddlSnapshotId);
        this.frontendSnapshotClient.advanceQuerySnapshot(
                snapshotId,
                graphDef,
                new CompletionCallback<Long>() {
                    @Override
                    public void onCompleted(Long res) {
                        if (res >= snapshotId) {
                            logger.warn(
                                    "Unexpected previous snapshot id [{}], should <= [{}], frontend"
                                            + " [{}]",
                                    res,
                                    snapshotId,
                                    frontendId);
                        } else {
                            lastDdlSnapshotId.getAndUpdate(x -> Math.max(x, ddlSnapshotId));
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.error(
                                "error in advanceQuerySnapshot [{}]-[{}], frontend [{}]",
                                snapshotId,
                                ddlSnapshotId,
                                frontendId,
                                t);
                    }
                });
    }
}
