/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.servers.gaia;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import org.apache.commons.lang3.tuple.Pair;

public class CachedMaxGraphMeta {
    private SchemaFetcher schemaFetcher;
    private GraphSchema graphSchema;
    private Long snapshotId;

    public CachedMaxGraphMeta(SchemaFetcher schemaFetcher) {
        this.schemaFetcher = schemaFetcher;
    }

    public synchronized void update() {
        Pair<GraphSchema, Long> pair = this.schemaFetcher.getSchemaSnapshotPair();
        if (pair != null) {
            this.graphSchema = pair.getLeft();
            this.snapshotId = pair.getRight();
        }
    }

    public GraphSchema getGraphSchema() {
        return graphSchema;
    }

    public Long getSnapshotId() {
        return snapshotId;
    }
}
