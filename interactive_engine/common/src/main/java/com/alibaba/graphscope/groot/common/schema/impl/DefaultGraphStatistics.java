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
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DefaultGraphStatistics implements GraphStatistics {
    private static final Logger logger = LoggerFactory.getLogger(DefaultGraphStatistics.class);
    private Map<LabelId, Long> vertexTypeCounts = Maps.newHashMap();
    private Map<EdgeKind, Long> edgeTypeCounts = Maps.newHashMap();
    private Long totalVertexCount;
    private Long totalEdgeCount;

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
    public String getVersion() {
        return "0";
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
    public Long getVertexTypeCount(Integer vertexTypeId) {
        return vertexTypeCounts.get(new LabelId(vertexTypeId));
    }

    @Override
    public Long getEdgeTypeCount(
            Optional<Integer> sourceTypeId,
            Optional<Integer> edgeTypeId,
            Optional<Integer> targetTypeId) {
        Long count =
                edgeTypeCounts.entrySet().stream()
                        .filter(
                                entry ->
                                        (!sourceTypeId.isPresent()
                                                || entry.getKey()
                                                        .getSrcVertexLabelId()
                                                        .equals(new LabelId(sourceTypeId.get()))))
                        .filter(
                                entry ->
                                        (!edgeTypeId.isPresent()
                                                || entry.getKey()
                                                        .getEdgeLabelId()
                                                        .equals(new LabelId(edgeTypeId.get()))))
                        .filter(
                                entry ->
                                        (!targetTypeId.isPresent()
                                                || entry.getKey()
                                                        .getDstVertexLabelId()
                                                        .equals(new LabelId(targetTypeId.get()))))
                        .collect(Collectors.summingLong(Map.Entry::getValue));

        return count == null ? 0L : count;
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
