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
package com.alibaba.graphscope.groot.schema;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.groot.schema.mapper.DefaultGraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;
import com.alibaba.graphscope.groot.schema.mapper.EdgeTypeMapper;
import com.alibaba.graphscope.groot.schema.mapper.SchemaElementMapper;
import com.alibaba.graphscope.groot.schema.mapper.VertexTypeMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

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
        Map<String, GraphVertex> vertexTypeMap = Maps.newHashMap();
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
        schemaMapper.types = Lists.newArrayList();
        for (GraphVertex graphVertex : schema.getVertexList()) {
            schemaMapper.types.add(VertexTypeMapper.parseFromVertexType(graphVertex));
        }
        for (GraphEdge graphEdge : schema.getEdgeList()) {
            schemaMapper.types.add(EdgeTypeMapper.parseFromEdgeType(graphEdge));
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
