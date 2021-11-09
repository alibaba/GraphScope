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

import com.alibaba.graphscope.groot.SnapshotCache;
import com.alibaba.graphscope.groot.schema.GraphDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class LocalSnapshotListener implements QuerySnapshotListener {
    private static final Logger logger = LoggerFactory.getLogger(LocalSnapshotListener.class);

    private SchemaManager schemaManager;
    private SnapshotCache snapshotCache;
    private AtomicLong lastDdlSnapshotId;

    public LocalSnapshotListener(SchemaManager schemaManager, SnapshotCache snapshotCache) {
        this.schemaManager = schemaManager;
        this.snapshotCache = snapshotCache;
        this.lastDdlSnapshotId = new AtomicLong(-1L);
    }

    @Override
    public void snapshotAdvanced(long snapshotId, long ddlSnapshotId) {
        logger.debug(
                "snapshot advance to ["
                        + snapshotId
                        + "]-["
                        + ddlSnapshotId
                        + "], will update local snapshot cache");
        GraphDef graphDef = null;
        if (ddlSnapshotId > this.lastDdlSnapshotId.get()) {
            graphDef = this.schemaManager.getGraphDef();
        }
        this.snapshotCache.advanceQuerySnapshotId(snapshotId, graphDef);
        lastDdlSnapshotId.getAndUpdate(
                x -> Math.max(x, ddlSnapshotId));
    }
}
