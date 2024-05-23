/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.common.schema.impl;

import com.alibaba.graphscope.groot.common.schema.api.GraphEdge;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.schema.api.GraphStatistics;
import com.alibaba.graphscope.groot.common.schema.api.GraphVertex;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DefaultGraphStatistics implements GraphStatistics {
    private static final Logger logger = LoggerFactory.getLogger(DefaultGraphStatistics.class);
    private Map<GraphVertex, Integer> vertexTypeCounts = Maps.newHashMap();
    private  Map<GraphEdge, Integer> edgeTypeCounts = Maps.newHashMap();
    private  Integer totalVertexCount;
    private  Integer totalEdgeCount;

    public DefaultGraphStatistics(
            Map<GraphVertex, Integer> vertexTypeCounts,
            Map<GraphEdge, Integer> edgeTypeCounts,
            Integer totalVertexCount,
            Integer totalEdgeCount) {
        this.vertexTypeCounts = vertexTypeCounts;
        this.edgeTypeCounts = edgeTypeCounts;
        this.totalVertexCount = totalVertexCount;
        this.totalEdgeCount = totalEdgeCount;
    }

    public static GraphSchema buildSchemaFromJson(String schemaJson) {
        // TODO: implement this method
          throw new UnsupportedOperationException("Unimplemented method 'buildSchemaFromJson'");
      }

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public Double getVertexCount() {
        return totalVertexCount.doubleValue();
    }

    @Override
    public Double getEdgeCount() {
        return totalEdgeCount.doubleValue();
    }

    @Override
    public Double getVertexTypeCount(GraphVertex vertex) {
        return vertexTypeCounts.get(vertex).doubleValue();
    }

    @Override
    public Double getEdgeTypeCount(GraphEdge edge) {
        return edgeTypeCounts.get(edge).doubleValue();
    }
}
