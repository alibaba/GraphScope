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
package com.alibaba.graphscope.groot.frontend;

import com.alibaba.graphscope.groot.SnapshotCache;
import com.alibaba.graphscope.groot.SnapshotWithSchema;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.schema.api.SchemaFetcher;
import com.alibaba.graphscope.groot.meta.MetaService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class WrappedSchemaFetcher implements SchemaFetcher {
    private static final Logger logger = LoggerFactory.getLogger(WrappedSchemaFetcher.class);

    private final SnapshotCache snapshotCache;
    private final MetaService metaService;
    // If this is a secondary instance, then always use the latest snapshot ID.
    private final boolean isSecondary;

    public WrappedSchemaFetcher(
            SnapshotCache snapshotCache, MetaService metaService, boolean isSecondary) {
        this.snapshotCache = snapshotCache;
        this.metaService = metaService;
        this.isSecondary = isSecondary;
    }

    @Override
    public Map<Long, GraphSchema> getSchemaSnapshotPair() {
        SnapshotWithSchema snapshotSchema = this.snapshotCache.getSnapshotWithSchema();
        long MAX_SNAPSHOT_ID = Long.MAX_VALUE - 1;
        // Always retrieve the latest result in secondary instance
        long snapshotId = isSecondary ? MAX_SNAPSHOT_ID : snapshotSchema.getSnapshotId();
        GraphSchema schema = snapshotSchema.getGraphDef();
        return Map.of(snapshotId, schema);
    }

    @Override
    public int getPartitionNum() {
        return this.metaService.getPartitionCount();
    }

    @Override
    public int getVersion() {
        throw new UnsupportedOperationException();
    }
}
