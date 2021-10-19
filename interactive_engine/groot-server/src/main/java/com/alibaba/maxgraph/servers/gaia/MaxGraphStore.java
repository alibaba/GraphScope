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

import com.alibaba.graphscope.common.proto.GremlinResult;
import com.alibaba.graphscope.gaia.store.GraphElementId;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.graphscope.gaia.store.SchemaNotFoundException;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxGraphStore extends GraphStoreService {
    private static final Logger logger = LoggerFactory.getLogger(MaxGraphStore.class);
    public static final String MAXGRAPH_MODERN_PROPERTY_RESOURCE =
            "maxgraph.modern.properties.json";
    private CachedMaxGraphMeta cachedGraphSchemaPair;

    public MaxGraphStore(SchemaFetcher schemaFetcher) {
        super(MAXGRAPH_MODERN_PROPERTY_RESOURCE);
        this.cachedGraphSchemaPair = new CachedMaxGraphMeta(schemaFetcher);
    }

    @Override
    public long getLabelId(String label) {
        try {
            GraphSchema graphSchema = this.cachedGraphSchemaPair.getGraphSchema();
            return graphSchema.getElement(label).getLabelId();
        } catch (Exception e) {
            logger.error("label " + label + " is invalid, please check schema");
            throw new SchemaNotFoundException(
                    "label " + label + " is invalid, please check schema");
        }
    }

    @Override
    public String getLabel(long labelId) {
        GraphSchema graphSchema = this.cachedGraphSchemaPair.getGraphSchema();
        return graphSchema.getElement((int) labelId).getLabel();
    }

    @Override
    public long getGlobalId(long labelId, long propertyId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getPropertyId(String propertyName) {
        try {
            GraphSchema graphSchema = this.cachedGraphSchemaPair.getGraphSchema();
            return graphSchema.getPropertyId(propertyName);
        } catch (Exception e) {
            logger.error("property " + propertyName + " is invalid, please check schema");
            throw new SchemaNotFoundException(
                    "property " + propertyName + " is invalid, please check schema");
        }
    }

    @Override
    public String getPropertyName(int propertyId) {
        GraphSchema graphSchema = this.cachedGraphSchemaPair.getGraphSchema();
        return graphSchema.getPropertyName(propertyId);
    }

    @Override
    public long getSnapShotId() {
        return this.cachedGraphSchemaPair.getSnapshotId();
    }

    @Override
    public synchronized void updateSnapShotId() {
        this.cachedGraphSchemaPair.update();
    }

    @Override
    public Object getCompositeId(GremlinResult.Edge edge) {
        return String.format("%d_%d", edge.getSrcId(), edge.getDstId());
    }

    @Override
    public Object fromBytes(byte[] edgeId) {
        Long value = 0L;
        if (edgeId.length < GraphElementId.BYTE_SIZE) {
            logger.error("invalid edge id array {}, use zero as default", edgeId);
            return value;
        }
        for (int i = Long.BYTES; i < GraphElementId.BYTE_SIZE; ++i) {
            value = (value << 8) + (0x00FF & edgeId[i]);
        }
        return value;
    }
}
