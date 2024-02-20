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

package com.alibaba.graphscope.gremlin.integration.resultx;

import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gremlin.resultx.GremlinRecordParser;
import com.alibaba.graphscope.gremlin.resultx.ResultSchema;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.type.RelDataType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GremlinTestRecordParser extends GremlinRecordParser {
    private static final String VERTEX_PROPERTIES = "vertex_properties";
    private static final String EDGE_PROPERTIES = "edge_properties";
    private final Map<String, Object> cachedProperties;

    public GremlinTestRecordParser(
            ResultSchema resultSchema, Map<String, Object> cachedProperties) {
        super(resultSchema);
        this.cachedProperties = cachedProperties;
    }

    @Override
    protected Map<String, Object> parseVertexProperties(IrResult.Vertex vertex, RelDataType type) {
        Map<String, Object> vertexProperties =
                (Map<String, Object>) cachedProperties.get(VERTEX_PROPERTIES);
        String idAsStr = String.valueOf(vertex.getId());
        Map<String, Object> properties = (Map<String, Object>) vertexProperties.get(idAsStr);
        if (properties != null) {
            Map<String, Object> formatProperties = new HashMap<>();
            properties.forEach(
                    (k, v) -> {
                        formatProperties.put(
                                k,
                                Collections.singletonList(ImmutableMap.of("id", 1L, "value", v)));
                    });
            return formatProperties;
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    protected Map<String, Object> parseEdgeProperties(IrResult.Edge edge, RelDataType type) {
        Map<String, Object> edgeProperties =
                (Map<String, Object>) cachedProperties.get(EDGE_PROPERTIES);
        String idAsStr = String.valueOf(edge.getId());
        Map<String, Object> properties = (Map<String, Object>) edgeProperties.get(idAsStr);
        return (properties == null) ? Collections.emptyMap() : properties;
    }
}
