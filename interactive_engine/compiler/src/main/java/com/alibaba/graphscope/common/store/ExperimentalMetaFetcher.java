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

package com.alibaba.graphscope.common.store;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.common.ir.schema.GraphSchemaWrapper;
import com.alibaba.graphscope.compiler.api.schema.*;
import com.alibaba.graphscope.compiler.schema.*;
import com.alibaba.graphscope.gremlin.Utils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.*;

public class ExperimentalMetaFetcher implements IrMetaFetcher {
    private final IrMeta meta;

    public ExperimentalMetaFetcher(Configs configs) throws IOException {
        String schemaFilePath = GraphConfig.GRAPH_SCHEMA.get(configs);
        String schemaJson = Utils.readStringFromFile(schemaFilePath);
        this.meta =
                new IrMeta(
                        new GraphSchemaWrapper(buildSchemaFromJson(schemaJson), false), schemaJson);
    }

    private GraphSchema buildSchemaFromJson(String schemaJson) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode jsonNode = mapper.readTree(schemaJson);
            Map<String, GraphVertex> vertexMap = Maps.newHashMap();
            Map<String, GraphEdge> edgeMap = Maps.newHashMap();
            Map<String, Integer> propNameToIdMap = Maps.newHashMap();
            buildGraphElementFromJson(
                    jsonNode.get("entities"), "VERTEX", vertexMap, edgeMap, propNameToIdMap);
            buildGraphElementFromJson(
                    jsonNode.get("relations"), "EDGE", vertexMap, edgeMap, propNameToIdMap);
            return new DefaultGraphSchema(vertexMap, edgeMap, propNameToIdMap);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void buildGraphElementFromJson(
            JsonNode elementList,
            String type,
            Map<String, GraphVertex> vertexMap,
            Map<String, GraphEdge> edgeMap,
            Map<String, Integer> propNameToIdMap) {
        Objects.requireNonNull(elementList);
        Iterator var1 = elementList.iterator();

        while (var1.hasNext()) {
            JsonNode typeObject = (JsonNode) var1.next();
            int labelId = typeObject.get("label").get("id").asInt();
            String label = typeObject.get("label").get("name").asText();
            List<GraphProperty> propertyList = Lists.newArrayList();
            List<GraphProperty> primaryPropertyList = Lists.newArrayList();
            JsonNode columnArray = typeObject.get("columns");
            Objects.requireNonNull(columnArray, "There's no property def list in " + label);
            Iterator var2 = columnArray.iterator();

            while (var2.hasNext()) {
                JsonNode column = (JsonNode) var2.next();
                String propName = column.get("key").get("name").asText();
                int propId = column.get("key").get("id").asInt();
                propNameToIdMap.put(propName, propId);
                int propTypeId = column.get("data_type").asInt();
                GraphProperty property =
                        new DefaultGraphProperty(propId, propName, toDataType(propTypeId));
                propertyList.add(property);
                boolean isPrimaryKey = column.get("is_primary_key").asBoolean();
                if (isPrimaryKey) {
                    primaryPropertyList.add(property);
                }
            }
            if (type.equals("EDGE")) {
                JsonNode entityPairs = typeObject.get("entity_pairs");
                Iterator var3 = entityPairs.iterator();
                List<EdgeRelation> relations = Lists.newArrayList();
                while (var3.hasNext()) {
                    JsonNode pair = (JsonNode) var3.next();
                    String sourceLabel = pair.get("src").get("name").asText();
                    String dstLabel = pair.get("dst").get("name").asText();
                    relations.add(
                            new DefaultEdgeRelation(
                                    vertexMap.get(sourceLabel), vertexMap.get(dstLabel)));
                }
                edgeMap.put(label, new DefaultGraphEdge(labelId, label, propertyList, relations));
            } else {
                vertexMap.put(
                        label,
                        new DefaultGraphVertex(labelId, label, propertyList, primaryPropertyList));
            }
        }
    }

    private DataType toDataType(int ordinal) {
        switch (ordinal) {
            case 0:
                return DataType.BOOL;
            case 1:
                return DataType.INT;
            case 2:
                return DataType.LONG;
            case 3:
                return DataType.DOUBLE;
            case 4:
                return DataType.STRING;
            case 5:
                return DataType.BYTES;
            case 6:
                return DataType.INT_LIST;
            case 7:
                return DataType.LONG_LIST;
            case 8:
                return DataType.DOUBLE_LIST;
            case 9:
                return DataType.STRING_LIST;
            default:
                return DataType.UNKNOWN;
        }
    }

    @Override
    public Optional<IrMeta> fetch() {
        return Optional.of(this.meta);
    }
}
