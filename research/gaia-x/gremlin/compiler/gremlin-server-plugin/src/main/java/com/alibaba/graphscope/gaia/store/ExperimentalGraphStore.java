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
package com.alibaba.graphscope.gaia.store;

import com.alibaba.graphscope.common.proto.GremlinResult;
import com.alibaba.graphscope.gaia.JsonUtils;
import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.idmaker.IdMaker;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ExperimentalGraphStore extends GraphStoreService {
    private static final Logger logger = LoggerFactory.getLogger(ExperimentalGraphStore.class);

    public static final String VERTEX_TYPE_MAP = "vertex_type_map";
    public static final String EDGE_TYPE_MAP = "edge_type_map";
    public static final String MODERN_PROPERTY_RESOURCE = "modern.properties.json";

    private Map<String, Object> graphSchema;
    private IdMaker idMaker;

    public ExperimentalGraphStore(GaiaConfig config) {
        super(MODERN_PROPERTY_RESOURCE);
        try {
            File configF = new File(config.getSchemaFilePath());
            String schemaJson = FileUtils.readFileToString(configF, StandardCharsets.UTF_8);
            this.graphSchema = JsonUtils.fromJson(schemaJson, new TypeReference<Map<String, Object>>() {
            });
            this.idMaker = new GlobalIdMaker();
        } catch (Exception e) {
            throw new RuntimeException("exception is ", e);
        }
    }

    @Override
    public long getLabelId(String label) {
        Map<String, Integer> vertexTypeMap = (Map<String, Integer>) graphSchema.get(VERTEX_TYPE_MAP);
        Integer typeId;
        if (vertexTypeMap != null && (typeId = vertexTypeMap.get(label)) != null) {
            return typeId;
        }
        Integer edgeTypeId;
        Map<String, Integer> edgeTypeMap = (Map<String, Integer>) graphSchema.get(EDGE_TYPE_MAP);
        if (edgeTypeMap != null && (edgeTypeId = edgeTypeMap.get(label)) != null) {
            return edgeTypeId;
        }
        throw new RuntimeException("label " + label + " is invalid, please check schema");
    }

    @Override
    public long getGlobalId(long labelId, long propertyId) {
        return (Long) idMaker.getId(Arrays.asList(labelId, propertyId));
    }

    @Override
    public Object getCompositeId(GremlinResult.Edge edge) {
        return fromBytes(edge.getId().toByteArray());
    }

    @Override
    public Object fromBytes(byte[] edgeId) {
        return new BigInteger(edgeId);
    }

    @Override
    public String getLabel(long labelId) {
        Map<String, Integer> vertexTypeMap = (Map<String, Integer>) graphSchema.get(VERTEX_TYPE_MAP);
        String label = null;
        for (Map.Entry<String, Integer> e : vertexTypeMap.entrySet()) {
            if (e.getValue() == labelId) {
                label = e.getKey();
            }
        }
        Map<String, Integer> edgeTypeMap = (Map<String, Integer>) graphSchema.get(EDGE_TYPE_MAP);
        for (Map.Entry<String, Integer> e : edgeTypeMap.entrySet()) {
            if (e.getValue() == labelId) {
                label = e.getKey();
            }
        }
        if (label == null) {
            throw new RuntimeException("labelId is invalid " + labelId);
        }
        return label;
    }
}
