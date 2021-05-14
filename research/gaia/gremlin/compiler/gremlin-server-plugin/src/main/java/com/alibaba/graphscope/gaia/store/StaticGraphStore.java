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
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class StaticGraphStore implements GraphStoreService {
    private static final Logger logger = LoggerFactory.getLogger(StaticGraphStore.class);

    public static final String VERTEX_TYPE_MAP = "vertex_type_map";
    public static final String EDGE_TYPE_MAP = "edge_type_map";
    public static final long INVALID_ID = -1L;

    private Map<String, Object> graphSchema;
    private GlobalIdMaker idMaker;
    public static final StaticGraphStore INSTANCE = new StaticGraphStore("conf/graph.properties");

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
        } catch (IOException e) {
            throw new RuntimeException("exception is ", e);
        }
    }

    @Override
    public long getLabelId(String label) {
        Map<String, Integer> vertexTypeMap = (Map<String, Integer>) graphSchema.get(VERTEX_TYPE_MAP);
//        // todo: for test, remove
//        label = label.toUpperCase();
        Integer typeId;
        if (vertexTypeMap != null && (typeId = vertexTypeMap.get(label)) != null) {
            return typeId;
        }
        Integer edgeTypeId;
        Map<String, Integer> edgeTypeMap = (Map<String, Integer>) graphSchema.get(EDGE_TYPE_MAP);
        if (edgeTypeMap != null && (edgeTypeId = edgeTypeMap.get(label)) != null) {
            return edgeTypeId;
        }
        return INVALID_ID;
    }

    @Override
    public long getGlobalId(long labelId, long propertyId) {
        long globalId = idMaker.makeId(Arrays.asList(labelId, propertyId));
        return globalId;
    }
}
