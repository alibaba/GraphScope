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
import com.alibaba.graphscope.proto.groot.Statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DefaultGraphStatistics implements GraphStatistics {
    private static final Logger logger = LoggerFactory.getLogger(DefaultGraphStatistics.class);
    private final Map<LabelId, Long> vertexTypeCounts;
    private final Map<EdgeKind, Long> edgeTypeCounts;
    private final Long totalVertexCount;
    private final Long totalEdgeCount;

    public DefaultGraphStatistics(
            Map<LabelId, Long> vertexTypeCounts,
            Map<EdgeKind, Long> edgeTypeCounts,
            Long totalVertexCount,
            Long totalEdgeCount) {
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
    public Long getVertexCount() {
        return totalVertexCount;
    }

    @Override
    public Long getEdgeCount() {
        return totalEdgeCount;
    }

    @Override
    public Long getVertexTypeCount(int vertexTypeId) {
        return vertexTypeCounts.get(new LabelId(vertexTypeId));
    }

    @Override
    public Long getEdgeTypeCount(int edgeTypeId, int sourceTypeId, int targetTypeId) {
        EdgeKind edgeKind =
                EdgeKind.newBuilder()
                        .setEdgeLabelId(new LabelId(edgeTypeId))
                        .setSrcVertexLabelId(new LabelId(sourceTypeId))
                        .setDstVertexLabelId(new LabelId(targetTypeId))
                        .build();
        return edgeTypeCounts.get(edgeKind);
    }

    public static DefaultGraphStatistics parseProto(Statistics statistics) {
        long vcount = statistics.getNumVertices();
        long ecount = statistics.getNumEdges();
        Map<LabelId, Long> vertexTypeCounts = new HashMap<>();
        Map<EdgeKind, Long> edgeTypeCounts = new HashMap<>();
        for (Statistics.VertexTypeStatistics sts : statistics.getVertexTypeStatisticsList()) {
            vertexTypeCounts.put(LabelId.parseProto(sts.getLabelId()), sts.getNumVertices());
        }
        for (Statistics.EdgeTypeStatistics sts : statistics.getEdgeTypeStatisticsList()) {
            edgeTypeCounts.put(EdgeKind.parseProto(sts.getEdgeKind()), sts.getNumEdges());
        }
        return new DefaultGraphStatistics(vertexTypeCounts, edgeTypeCounts, vcount, ecount);
    }

    @Override
    public String toString() {
        return "DefaultGraphStatistics{"
                + "vertexTypeCounts="
                + vertexTypeCounts
                + ", edgeTypeCounts="
                + edgeTypeCounts
                + ", totalVertexCount="
                + totalVertexCount
                + ", totalEdgeCount="
                + totalEdgeCount
                + '}';
    }
}
