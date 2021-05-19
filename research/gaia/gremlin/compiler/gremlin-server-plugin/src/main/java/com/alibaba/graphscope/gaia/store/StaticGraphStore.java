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

import com.alibaba.graphscope.gaia.JsonUtils;
import com.alibaba.graphscope.gaia.idmaker.IdMaker;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class StaticGraphStore implements GraphStoreService {
    private static final Logger logger = LoggerFactory.getLogger(StaticGraphStore.class);

    public static final String VERTEX_TYPE_MAP = "vertex_type_map";
    public static final String EDGE_TYPE_MAP = "edge_type_map";
    public static final long INVALID_ID = -1L;
    public static final StaticGraphStore INSTANCE = new StaticGraphStore("conf/graph.properties");

    private Map<String, Object> graphSchema;
    private IdMaker idMaker;
    private Map<String, Map<String, Map<String, Object>>> propertyData;

    private StaticGraphStore(String graphConfig) {
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(graphConfig));
            String schemaFromSys = System.getProperty("gremlin.graph.schema");
            if (schemaFromSys != null) {
                properties.put("gremlin.graph.schema", schemaFromSys);
            }
            File configF = new File((String) properties.get("gremlin.graph.schema"));
            String schemaJson = FileUtils.readFileToString(configF, StandardCharsets.UTF_8);
            this.graphSchema = JsonUtils.fromJson(schemaJson, new TypeReference<Map<String, Object>>() {
            });
            this.idMaker = new GlobalIdMaker(Collections.EMPTY_LIST);
            // init properties from file under resources
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("modern.properties.json");
            String propertiesJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            this.propertyData = JsonUtils.fromJson(propertiesJson, new TypeReference<Map<String, Map<String, Map<String, Object>>>>() {
            });
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
    public <P> Optional<P> getVertexProperty(long id, String key) {
        String idStr = String.valueOf(id);
        if (getVertexKeys(id).isEmpty()) return Optional.empty();
        return Optional.ofNullable((P) propertyData.get("vertex_properties").get(idStr).get(key));
    }

    @Override
    public Set<String> getVertexKeys(long id) {
        String idStr = String.valueOf(id);
        Map<String, Object> result = propertyData.get("vertex_properties").get(idStr);
        if (result == null) return Collections.EMPTY_SET;
        return result.keySet();
    }

    @Override
    public <P> Optional<P> getEdgeProperty(long id, String key) {
        String idStr = String.valueOf(id);
        if (getEdgeKeys(id).isEmpty()) return Optional.empty();
        return Optional.ofNullable((P) propertyData.get("edge_properties").get(idStr).get(key));
    }

    @Override
    public Set<String> getEdgeKeys(long id) {
        String idStr = String.valueOf(id);
        Map<String, Object> result = propertyData.get("edge_properties").get(idStr);
        if (result == null) return Collections.EMPTY_SET;
        return result.keySet();
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
