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
package com.alibaba.graphscope.groot.common.schema.mapper;

import com.alibaba.graphscope.groot.common.exception.InvalidArgumentException;
import com.alibaba.graphscope.groot.common.schema.api.GraphEdge;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.schema.api.GraphVertex;
import com.alibaba.graphscope.groot.common.schema.impl.DefaultGraphSchema;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
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
    private String version;

    public List<SchemaElementMapper> getTypes() {
        return types;
    }

    public String getVersion() {
        return version;
    }

    public GraphSchema toGraphSchema() {
        Map<String, GraphVertex> vertexTypeMap = new HashMap<>();
        Map<String, GraphEdge> edgeTypeMap = new HashMap<>();
        Map<String, Integer> propNameToIdList = new HashMap<>();
        for (SchemaElementMapper elementMapper : this.types) {
            if (elementMapper instanceof VertexTypeMapper) {
                GraphVertex graphVertex = ((VertexTypeMapper) elementMapper).toVertexType();
                vertexTypeMap.put(graphVertex.getLabel(), graphVertex);
            } else {
                GraphEdge graphEdge = ((EdgeTypeMapper) elementMapper).toEdgeType(vertexTypeMap);
                edgeTypeMap.put(graphEdge.getLabel(), graphEdge);
            }
            for (GraphPropertyMapper def : elementMapper.getPropertyDefList()) {
                propNameToIdList.put(def.getName(), def.getId());
            }
        }
        return new DefaultGraphSchema(vertexTypeMap, edgeTypeMap, propNameToIdList);
    }

    public String toJsonString() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new InvalidArgumentException("convert graph schema to json failed", e);
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
                graphSchema.version = jsonNode.get("version").asText();
            } else {
                graphSchema.version = "0";
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
            throw new InvalidArgumentException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        String path = "groot-server/src/test/resources/schema.json";
        String schemaJson = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
        GraphSchema graphSchema = GraphSchemaMapper.parseFromJson(schemaJson).toGraphSchema();
        GraphSchemaMapper mapper = GraphSchemaMapper.parseFromSchema(graphSchema);
        System.out.println(mapper.toJsonString());
    }
}
