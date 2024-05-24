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

import com.alibaba.graphscope.groot.common.schema.api.GraphStatistics;
import com.alibaba.graphscope.groot.common.schema.wrapper.EdgeKind;
import com.alibaba.graphscope.groot.common.schema.wrapper.LabelId;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DefaultGraphStatistics implements GraphStatistics {
    private static final Logger logger = LoggerFactory.getLogger(DefaultGraphStatistics.class);
    private Map<LabelId, Integer> vertexTypeCounts = Maps.newHashMap();
    private Map<EdgeKind, Integer> edgeTypeCounts = Maps.newHashMap();
    private Integer totalVertexCount;
    private Integer totalEdgeCount;

    public DefaultGraphStatistics(
            Map<LabelId, Integer> vertexTypeCounts,
            Map<EdgeKind, Integer> edgeTypeCounts,
            Integer totalVertexCount,
            Integer totalEdgeCount) {
        this.vertexTypeCounts = vertexTypeCounts;
        this.edgeTypeCounts = edgeTypeCounts;
        this.totalVertexCount = totalVertexCount;
        this.totalEdgeCount = totalEdgeCount;
    }

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public Integer getVertexCount() {
        return totalVertexCount;
    }

    @Override
    public Integer getEdgeCount() {
        return totalEdgeCount;
    }

    @Override
    public Integer getVertexTypeCount(int vertexTypeId) {
        return vertexTypeCounts.get(new LabelId(vertexTypeId));
    }

    @Override
    public Integer getEdgeTypeCount(int edgeTypeId, int sourceTypeId, int targetTypeId) {
        EdgeKind edgeKind =
                EdgeKind.newBuilder()
                        .setEdgeLabelId(new LabelId(edgeTypeId))
                        .setSrcVertexLabelId(new LabelId(sourceTypeId))
                        .setDstVertexLabelId(new LabelId(targetTypeId))
                        .build();
        return edgeTypeCounts.get(edgeKind);
    }
}
