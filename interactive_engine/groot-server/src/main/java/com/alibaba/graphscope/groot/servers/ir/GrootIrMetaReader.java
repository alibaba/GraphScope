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

package com.alibaba.graphscope.groot.servers.ir;

import com.alibaba.graphscope.common.ir.meta.GraphId;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.SnapshotId;
import com.alibaba.graphscope.common.ir.meta.reader.IrMetaReader;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.schema.api.GraphStatistics;
import com.alibaba.graphscope.groot.common.schema.api.SchemaFetcher;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.*;

public class GrootIrMetaReader implements IrMetaReader {
    private final SchemaFetcher schemaFetcher;

    public GrootIrMetaReader(SchemaFetcher schemaFetcher) {
        this.schemaFetcher = schemaFetcher;
    }

    @Override
    public IrMeta readMeta() throws IOException {
        Map<Long, GraphSchema> pair = this.schemaFetcher.getSchemaSnapshotPair();
        Preconditions.checkArgument(!pair.isEmpty(), "fetch schema snapshot pair failed in groot");
        Map.Entry<Long, GraphSchema> entry = pair.entrySet().iterator().next();
        Long snapshotId = entry.getKey();
        GraphSchema schema = entry.getValue();
        return new IrMeta(new SnapshotId(true, snapshotId), new IrGraphSchema(schema, true));
    }

    @Override
    public GraphStatistics readStats(GraphId graphId) throws IOException {
        return schemaFetcher.getStatistics();
    }

    @Override
    public boolean syncStatsEnabled(GraphId graphId) {
        return schemaFetcher.statisticsEnabled();
    }
}
