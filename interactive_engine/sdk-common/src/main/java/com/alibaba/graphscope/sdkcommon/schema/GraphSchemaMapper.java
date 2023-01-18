/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.sdkcommon.schema;

import com.alibaba.graphscope.compiler.api.schema.GraphEdge;
import com.alibaba.graphscope.compiler.api.schema.GraphSchema;
import com.alibaba.graphscope.compiler.api.schema.GraphVertex;
import com.alibaba.graphscope.sdkcommon.schema.mapper.DefaultGraphSchema;
import com.alibaba.graphscope.sdkcommon.schema.mapper.EdgeTypeMapper;
import com.alibaba.graphscope.sdkcommon.schema.mapper.SchemaElementMapper;
import com.alibaba.graphscope.sdkcommon.schema.mapper.VertexTypeMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * GraphSchemaMapper will convert graph schema to json string and parse json string to graph schema
 * mapper
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
        Map<String, GraphVertex> vertexTypeMap = new HashMap<>();
        for (SchemaElementMapper elementMapper : this.types) {
            if (elementMapper instanceof VertexTypeMapper) {
                GraphVertex graphVertex = ((VertexTypeMapper) elementMapper).toVertexType();
                graphSchema.createVertexType(graphVertex);
                vertexTypeMap.put(graphVertex.getLabel(), graphVertex);
            }
        }
        for (SchemaElementMapper elementMapper : this.types) {
            if (elementMapper instanceof EdgeTypeMapper) {
                GraphEdge graphEdge = ((EdgeTypeMapper) elementMapper).toEdgeType(vertexTypeMap);
                graphSchema.createEdgeType(graphEdge);
            }
        }

        return graphSchema;
    }

    public String toJsonString() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("convert graph schema to json failed", e);
        }
    }

    public static GraphSchemaMapper parseFromSchema(GraphSchema schema) {
        GraphSchemaMapper schemaMapper = new GraphSchemaMapper();
        schemaMapper.version = schema.getVersion();
        schemaMapper.types = new ArrayList<>();
        for (GraphVertex graphVertex : schema.getVertexList()) {
            schemaMapper.types.add(VertexTypeMapper.parseFromVertexType(graphVertex));
        }
        for (GraphEdge graphEdge : schema.getEdgeList()) {
            schemaMapper.types.add(EdgeTypeMapper.parseFromEdgeType(graphEdge));
        }

        return schemaMapper;
    }

    public static GraphSchemaMapper parseFromJson(String schemaJson) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            GraphSchemaMapper graphSchema = new GraphSchemaMapper();
            JsonNode jsonNode = mapper.readTree(schemaJson);
            if (jsonNode.has("version")) {
                graphSchema.version = jsonNode.get("version").asInt();
            } else {
                graphSchema.version = 0;
            }
            graphSchema.types = new ArrayList<>();
            JsonNode typeArray = jsonNode.get("types");
            for (JsonNode typeNode : typeArray) {
                String type = typeNode.get("type").asText();
                if (type.equalsIgnoreCase("VERTEX")) {
                    VertexTypeMapper typeMapper =
                            mapper.convertValue(typeNode, VertexTypeMapper.class);
                    graphSchema.types.add(typeMapper);
                } else {
                    EdgeTypeMapper typeMapper = mapper.convertValue(typeNode, EdgeTypeMapper.class);
                    graphSchema.types.add(typeMapper);
                }
            }
            return graphSchema;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
