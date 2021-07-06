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
package com.alibaba.maxgraph.gaia;

import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;

public class VineyardGraphStore extends GraphStoreService {
    public static final String MODERN_PROPERTY_RESOURCE = "modern.properties.json";
    private GraphSchema graphSchema;
    private long snapshotId;

    public VineyardGraphStore(SchemaFetcher schemaFetcher) {
        super(MODERN_PROPERTY_RESOURCE);
        this.graphSchema = schemaFetcher.getSchemaSnapshotPair().getLeft();
        this.snapshotId = schemaFetcher.getSchemaSnapshotPair().getRight();
    }

    @Override
    public long getLabelId(String label) {
        return graphSchema.getElement(label).getLabelId();
    }

    @Override
    public String getLabel(long labelId) {
        return graphSchema.getElement((int) labelId).getLabel();
    }

    @Override
    public long getGlobalId(long labelId, long propertyId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getPropertyId(String propertyName) {
        return graphSchema.getPropertyId(propertyName);
    }

    @Override
    public String getPropertyName(int propertyId) {
        return graphSchema.getPropertyName(propertyId);
    }

    @Override
    public long getSnapshotId() {
        return this.snapshotId;
    }
}
