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
    private SchemaFetcher schemaFetcher;

    public VineyardGraphStore(SchemaFetcher schemaFetcher) {
        super(MODERN_PROPERTY_RESOURCE);
        this.schemaFetcher = schemaFetcher;
    }

    @Override
    public long getLabelId(String label) {
        GraphSchema graphSchema = this.schemaFetcher.getSchemaSnapshotPair().getLeft();
        return graphSchema.getElement(label).getLabelId();
    }

    @Override
    public String getLabel(long labelId) {
        GraphSchema graphSchema = this.schemaFetcher.getSchemaSnapshotPair().getLeft();
        return graphSchema.getElement((int) labelId).getLabel();
    }

    @Override
    public long getGlobalId(long labelId, long propertyId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getPropertyId(String propertyName) {
        GraphSchema graphSchema = this.schemaFetcher.getSchemaSnapshotPair().getLeft();
        return graphSchema.getPropertyId(propertyName);
    }

    @Override
    public String getPropertyName(int propertyId) {
        GraphSchema graphSchema = this.schemaFetcher.getSchemaSnapshotPair().getLeft();
        return graphSchema.getPropertyName(propertyId);
    }

    public long getSnapshotId() {
        long snapshotId = this.schemaFetcher.getSchemaSnapshotPair().getRight();
        return snapshotId;
    }
}
