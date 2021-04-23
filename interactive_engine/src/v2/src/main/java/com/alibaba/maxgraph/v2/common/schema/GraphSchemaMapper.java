package com.alibaba.maxgraph.v2.common.schema;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultGraphSchema;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.mapper.EdgeTypeMapper;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.mapper.SchemaElementMapper;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.mapper.VertexTypeMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * GraphSchemaMapper will convert graph schema to json string and parse json string to graph schema mapper
 */
public class GraphSchemaMapper {
    private List<SchemaElementMapper> types;
    private int version;

    public List<SchemaElementMapper> getTypes() {
        return types;
    }

    public int getVersion() {
        return version;
    }

    public GraphSchema toGraphSchema() {
        DefaultGraphSchema graphSchema = new DefaultGraphSchema();
        Map<String, VertexType> vertexTypeMap = Maps.newHashMap();
        for (SchemaElementMapper elementMapper : this.types) {
            if (elementMapper instanceof VertexTypeMapper) {
                VertexType vertexType = ((VertexTypeMapper) elementMapper).toVertexType();
                graphSchema.createVertexType(vertexType);
                vertexTypeMap.put(vertexType.getLabel(), vertexType);
            }
        }
        for (SchemaElementMapper elementMapper : this.types) {
            if (elementMapper instanceof EdgeTypeMapper) {
                EdgeType edgeType = ((EdgeTypeMapper) elementMapper).toEdgeType(vertexTypeMap);
                graphSchema.createEdgeType(edgeType);
            }
        }

        return graphSchema;
    }

    public String toJsonString() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("convert graph schema to json failed", e);
        }
    }

    public static GraphSchemaMapper parseFromSchema(GraphSchema schema) {
        GraphSchemaMapper schemaMapper = new GraphSchemaMapper();
        schemaMapper.version = schema.getVersion();
        schemaMapper.types = Lists.newArrayList();
        for (VertexType vertexType : schema.getVertexTypes()) {
            schemaMapper.types.add(VertexTypeMapper.parseFromVertexType(vertexType));
        }
        for (EdgeType edgeType : schema.getEdgeTypes()) {
            schemaMapper.types.add(EdgeTypeMapper.parseFromEdgeType(edgeType));
        }

        return schemaMapper;
    }

    public static GraphSchemaMapper parseFromJson(String schemaJson) {
        JSONObject jsonObject = JSONObject.parseObject(schemaJson);
        GraphSchemaMapper graphSchema = new GraphSchemaMapper();
        Integer version = jsonObject.getInteger("version");
        if (null != version) {
            graphSchema.version = version;
        } else {
            graphSchema.version = 0;
        }

        graphSchema.types = Lists.newArrayList();
        JSONArray typeArray = jsonObject.getJSONArray("types");
        for (int i = 0; i < typeArray.size(); i++) {
            JSONObject typeObject = typeArray.getJSONObject(i);
            String type = typeObject.getString("type");
            if (StringUtils.equals("VERTEX", StringUtils.upperCase(type))) {
                VertexTypeMapper vertexTypeMapper = typeObject.toJavaObject(VertexTypeMapper.class);
                graphSchema.types.add(vertexTypeMapper);
            } else {
                EdgeTypeMapper edgeTypeMapper = typeObject.toJavaObject(EdgeTypeMapper.class);
                graphSchema.types.add(edgeTypeMapper);
            }
        }

        return graphSchema;
    }
}
