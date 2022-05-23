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

package com.alibaba.graphscope.ir.maxgraph;

import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.common.utils.JsonUtils;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.compiler.api.schema.*;
import com.alibaba.maxgraph.compiler.schema.JsonFileSchemaFetcher;
import com.google.common.collect.ImmutableMap;

import java.util.*;
import java.util.stream.Collectors;

public class VineyardMetaFetcher implements IrMetaFetcher {
    private IrMeta irMeta;

    public VineyardMetaFetcher(InstanceConfig instanceConfig) {
        String schemaPath = instanceConfig.getVineyardSchemaPath();
        JsonFileSchemaFetcher fetcher = new JsonFileSchemaFetcher(schemaPath);
        GraphSchema graphSchema = fetcher.getSchemaSnapshotPair().getLeft();
        this.irMeta = new IrMeta(parseSchema(graphSchema));
    }

    @Override
    public Optional<IrMeta> fetch() {
        return Optional.of(irMeta);
    }

    private String parseSchema(GraphSchema graphSchema) {
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
        return JsonUtils.toJson(schemaMap);
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
                                                            "id",
                                                            src.getLabelId(),
                                                            "name",
                                                            src.getLabel()),
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
                                            typeId);
                                })
                        .collect(Collectors.toList());
        return ImmutableMap.of(
                "label", ImmutableMap.of("id", labelId, "name", label), "columns", columns);
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
