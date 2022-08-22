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

package com.alibaba.maxgraph.common.util;

import com.alibaba.maxgraph.compiler.api.schema.*;
import com.alibaba.maxgraph.sdkcommon.util.JSON;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// singleton to parse ir schema in json format from GraphSchema
public class IrSchemaParser {
    private static final Logger logger = LoggerFactory.getLogger(IrSchemaParser.class);

    private static IrSchemaParser instance = new IrSchemaParser();

    public static IrSchemaParser getInstance() {
        return instance;
    }

    private IrSchemaParser() {}

    public String parse(GraphSchema graphSchema) {
        List<GraphVertex> vertices = graphSchema.getVertexList();
        List<GraphEdge> edges = graphSchema.getEdgeList();
        List entities = new ArrayList();
        List relations = new ArrayList();
        vertices.forEach(
                v -> {
                    entities.add(getVertex(graphSchema, v));
                });
        edges.forEach(
                e -> {
                    relations.add(getEdge(graphSchema, e));
                });
        Map<String, Object> schemaMap =
                ImmutableMap.of(
                        "entities",
                        entities,
                        "relations",
                        relations,
                        "is_table_id",
                        true,
                        "is_column_id",
                        true);
        String json = JSON.toJson(schemaMap);
        // logger.info("graph schema is {}, ir schema is {}", graphSchema.formatJson(), json);
        return json;
    }

    private Map<String, Object> getVertex(GraphSchema graphSchema, GraphVertex vertex) {
        return getElement(graphSchema, vertex);
    }

    private Map<String, Object> getEdge(GraphSchema graphSchema, GraphEdge edge) {
        Map<String, Object> entity = new LinkedHashMap(getElement(graphSchema, edge));
        List<EdgeRelation> relations = edge.getRelationList();
        List entityPairs =
                relations.stream()
                        .map(
                                k -> {
                                    GraphVertex src = k.getSource();
                                    GraphVertex dst = k.getTarget();
                                    return ImmutableMap.of(
                                            "src",
                                            ImmutableMap.of(
                                                    "id", src.getLabelId(), "name", src.getLabel()),
                                            "dst",
                                            ImmutableMap.of(
                                                    "id",
                                                    dst.getLabelId(),
                                                    "name",
                                                    dst.getLabel()));
                                })
                        .collect(Collectors.toList());
        entity.put("entity_pairs", entityPairs);
        return entity;
    }

    private Map<String, Object> getElement(GraphSchema graphSchema, GraphElement entity) {
        String label = entity.getLabel();
        int labelId = entity.getLabelId();
        List<GraphProperty> properties = entity.getPropertyList();
        List<GraphProperty> primaryKeys = entity.getPrimaryKeyList();
        List columns =
                properties.stream()
                        .map(
                                k -> {
                                    int typeId = getDataTypeId(k.getDataType());
                                    String name = k.getName();
                                    int nameId = graphSchema.getPropertyId(name);
                                    return ImmutableMap.of(
                                            "key",
                                            ImmutableMap.of("id", nameId, "name", name),
                                            "data_type",
                                            typeId,
                                            "is_primary_key",
                                            isPrimaryKey(label, name, primaryKeys));
                                })
                        .collect(Collectors.toList());
        return ImmutableMap.of(
                "label", ImmutableMap.of("id", labelId, "name", label), "columns", columns);
    }

    private boolean isPrimaryKey(
            String label, String propertyName, List<GraphProperty> primaryKeys) {
        boolean isPrimaryKey = false;
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            for (GraphProperty key : primaryKeys) {
                if (StringUtils.equals(propertyName, key.getName())) {
                    isPrimaryKey = true;
                    break;
                }
            }
        }
        logger.debug("label {}, property {}, primary key is {}", label, propertyName, isPrimaryKey);
        return isPrimaryKey;
    }

    private int getDataTypeId(DataType dataType) {
        switch (dataType) {
            case BOOL:
                return 0;
            case INT:
                return 1;
            case LONG:
                return 2;
            case DOUBLE:
                return 3;
            case STRING:
                return 4;
            case BYTES:
                return 5;
            case INT_LIST:
                return 6;
            case LONG_LIST:
                return 7;
            case DOUBLE_LIST:
                return 8;
            case STRING_LIST:
                return 9;
            case UNKNOWN:
            default:
                return 11;
        }
    }
}
