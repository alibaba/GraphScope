package com.alibaba.maxgraph.servers.ir;

import com.alibaba.graphscope.common.store.StoreConfigs;
import com.alibaba.graphscope.common.utils.JsonUtils;
import com.alibaba.graphscope.gremlin.Utils;
import com.alibaba.graphscope.groot.schema.GraphSchemaMapper;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.compiler.api.schema.*;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GrootStoreConfigs implements StoreConfigs {
    private GraphSchema graphSchema;

    public GrootStoreConfigs(Configs configs) throws IOException {
        String schemaPath = SchemaConfig.SCHEMA_PATH.get(configs);
        String schemaJson = Utils.readStringFromFile(schemaPath);
        this.graphSchema = GraphSchemaMapper.parseFromJson(schemaJson).toGraphSchema();
    }

    @Override
    public Map<String, Object> getConfigs() {
        return ImmutableMap.of("graph.schema", parseSchema());
    }

    private String parseSchema() {
        List<GraphVertex> vertices = graphSchema.getVertexList();
        List<GraphEdge> edges = graphSchema.getEdgeList();
        List entities = new ArrayList();
        vertices.forEach(v -> {
            entities.add(getEntity(v));
        });
        edges.forEach(e -> {
            entities.add(getEntity(e));
        });
        Map<String, Object> schemaMap = ImmutableMap.of("entities", entities);
        return JsonUtils.toJson(schemaMap);
    }

    private Map<String, Object> getEntity(GraphElement entity) {
        String label = entity.getLabel().toUpperCase();
        int labelId = entity.getLabelId();
        List<GraphProperty> properties = entity.getPropertyList();
        List columns = properties.stream().map(k -> {
            int typeId = getDataTypeId(k.getDataType());
            String name = k.getName();
            int nameId = graphSchema.getPropertyId(name);
            return ImmutableMap.of(
                    "key", ImmutableMap.of("id", nameId, "name", name),
                    "data_type", typeId);
        }).collect(Collectors.toList());
        return ImmutableMap.of(
                "label", ImmutableMap.of("id", labelId, "name", label),
                "columns", columns);
    }

    private Map<String, Object> getEdge(GraphEdge edge) {
        Map<String, Object> entity = getEntity(edge);
        List<EdgeRelation> relations = edge.getRelationList();
        List entityPairs = relations.stream().map(k -> {
            GraphVertex src = k.getSource();
            GraphVertex dst = k.getTarget();
            return ImmutableMap.of(
                    "src", ImmutableMap.of("id", src.getLabelId(), "name", src.getLabel().toUpperCase()),
                    "dst", ImmutableMap.of("id", dst.getLabelId(), "name", dst.getLabel().toUpperCase()));
        }).collect(Collectors.toList());
        entity.put("entity_pairs", entityPairs);
        return entity;
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
