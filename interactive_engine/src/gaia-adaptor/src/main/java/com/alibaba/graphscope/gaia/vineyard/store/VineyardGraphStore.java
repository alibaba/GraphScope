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
package com.alibaba.graphscope.gaia.vineyard.store;

import com.alibaba.graphscope.common.proto.GremlinResult;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.graphscope.gaia.store.SchemaNotFoundException;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

public class VineyardGraphStore extends GraphStoreService {
    private static final Logger logger = LoggerFactory.getLogger(VineyardGraphStore.class);
    public static final String VINEYARD_MODERN_PROPERTY_RESOURCE = "vineyard.modern.properties.json";
    private SchemaFetcher schemaFetcher;

    public VineyardGraphStore(SchemaFetcher schemaFetcher) {
        super(VINEYARD_MODERN_PROPERTY_RESOURCE);
        this.schemaFetcher = schemaFetcher;
    }

    @Override
    public long getLabelId(String label) {
        try {
            GraphSchema graphSchema = this.schemaFetcher.getSchemaSnapshotPair().getLeft();
            return graphSchema.getElement(label).getLabelId();
        } catch (Exception e) {
            logger.error("label " + label + " is invalid, please check schema");
            throw new SchemaNotFoundException("label " + label + " is invalid, please check schema");
        }
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
        try {
            GraphSchema graphSchema = this.schemaFetcher.getSchemaSnapshotPair().getLeft();
            return graphSchema.getPropertyId(propertyName);
        } catch (Exception e) {
            logger.error("property " + propertyName + " is invalid, please check schema");
            throw new SchemaNotFoundException("property " + propertyName + " is invalid, please check schema");
        }
    }

    @Override
    public String getPropertyName(int propertyId) {
        GraphSchema graphSchema = this.schemaFetcher.getSchemaSnapshotPair().getLeft();
        return graphSchema.getPropertyName(propertyId);
    }

    @Override
    public long getSnapShotId() {
        return this.schemaFetcher.getSchemaSnapshotPair().getRight();
    }

    @Override
    public synchronized void updateSnapShotId() {
    }

    @Override
    public Object fromBytes(byte[] edgeId) {
        return new BigInteger(edgeId);
    }

    @Override
    public Object getCompositeId(GremlinResult.Edge edge) {
        return String.format("%d_%d", edge.getSrcId(), edge.getDstId());
    }
}
