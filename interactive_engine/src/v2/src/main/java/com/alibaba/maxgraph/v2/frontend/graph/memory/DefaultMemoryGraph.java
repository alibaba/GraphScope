package com.alibaba.maxgraph.v2.frontend.graph.memory;

import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Memory graph for testing
 */
public class DefaultMemoryGraph {
    private static final DefaultMemoryGraph GRAPH = new DefaultMemoryGraph();
    private Map<Integer, Map<Long, Map<String, Object>>> labelVertexList = Maps.newHashMap();
    private Map<Integer, Map<Long, List<DefaultMemoryEdge>>> labelVertexOutEdgeList = Maps.newHashMap();
    private Map<Integer, Map<Long, List<DefaultMemoryEdge>>> labelVertexInEdgeList = Maps.newHashMap();

    public static DefaultMemoryGraph getGraph() {
        return GRAPH;
    }

    public void addVertex(ElementId vid, Map<String, Object> properties) {
        Map<Long, Map<String, Object>> vertexList = labelVertexList.computeIfAbsent(vid.labelId(), k -> Maps.newHashMap());
        vertexList.put(vid.id(), properties);
    }

    public void deleteVertex(ElementId vid) {
        this.labelVertexList.getOrDefault(vid.labelId(), Maps.newHashMap()).remove(vid.id());
        this.labelVertexOutEdgeList.getOrDefault(vid.labelId(), Maps.newHashMap()).remove(vid.id());
        this.labelVertexInEdgeList.getOrDefault(vid.labelId(), Maps.newHashMap()).remove(vid.id());
    }

    public void addEdge(ElementId srcId, ElementId destId, ElementId edgeId, Map<String, Object> properties) {
        DefaultMemoryEdge edge = new DefaultMemoryEdge(srcId, destId, edgeId, properties);
        this.labelVertexOutEdgeList.computeIfAbsent(srcId.labelId(), k -> Maps.newHashMap())
                .computeIfAbsent(srcId.id(), k -> Lists.newArrayList())
                .add(edge);
        this.labelVertexInEdgeList.computeIfAbsent(destId.labelId(), k -> Maps.newHashMap())
                .computeIfAbsent(destId.id(), k -> Lists.newArrayList())
                .add(edge);
    }

    public void deleteEdge(ElementId srcId, ElementId destId, ElementId edgeId) {
        this.labelVertexOutEdgeList.getOrDefault(srcId.labelId(), Maps.newHashMap())
                .getOrDefault(srcId.id(), Lists.newArrayList())
                .removeIf(v -> v.edgeId.id() == edgeId.id() && v.edgeId.labelId() == edgeId.labelId());
        this.labelVertexInEdgeList.getOrDefault(destId.labelId(), Maps.newHashMap())
                .getOrDefault(destId.id(), Lists.newArrayList())
                .removeIf(v -> v.edgeId.id() == edgeId.id() && v.edgeId.labelId() == edgeId.labelId());
    }

    public Map<String, Object> getVertexProperties(ElementId vid) {
        return this.labelVertexList.getOrDefault(vid.labelId(), Maps.newHashMap())
                .get(vid.id());
    }

    public List<ElementId> getOutVertices(ElementId vertexId) {
        List<DefaultMemoryEdge> edgeList = this.labelVertexOutEdgeList.computeIfAbsent(vertexId.labelId(), k -> Maps.newHashMap())
                .get(vertexId.id());
        if (null == edgeList || edgeList.isEmpty()) {
            return Lists.newArrayList();
        } else {
            return edgeList.stream().map(v -> v.destId).collect(Collectors.toList());
        }
    }

    public List<ElementId> getOutVertices(ElementId vertexId, Set<Integer> edgeLabelIdSet) {
        List<DefaultMemoryEdge> edgeList = this.labelVertexOutEdgeList.computeIfAbsent(vertexId.labelId(), k -> Maps.newHashMap())
                .get(vertexId.id());
        if (null == edgeList || edgeList.isEmpty()) {
            return Lists.newArrayList();
        } else {
            return edgeList.stream()
                    .filter(v -> edgeLabelIdSet.contains(v.edgeId.labelId()))
                    .map(v -> v.destId)
                    .collect(Collectors.toList());
        }
    }

    public List<ElementId> getInVertices(ElementId vertexId) {
        List<DefaultMemoryEdge> edgeList = this.labelVertexInEdgeList.computeIfAbsent(vertexId.labelId(), k -> Maps.newHashMap())
                .get(vertexId.id());
        if (null == edgeList || edgeList.isEmpty()) {
            return Lists.newArrayList();
        } else {
            return edgeList.stream().map(v -> v.srcId).collect(Collectors.toList());
        }
    }

    public List<ElementId> getInVertices(ElementId vertexId, Set<Integer> edgeLabelIdSet) {
        List<DefaultMemoryEdge> edgeList = this.labelVertexInEdgeList.computeIfAbsent(vertexId.labelId(), k -> Maps.newHashMap())
                .get(vertexId.id());
        if (null == edgeList || edgeList.isEmpty()) {
            return Lists.newArrayList();
        } else {
            return edgeList.stream()
                    .filter(v -> edgeLabelIdSet.contains(v.edgeId.labelId()))
                    .map(v -> v.srcId).collect(Collectors.toList());
        }
    }

    public Map<Integer, Map<Long, Map<String, Object>>> scanVertices(Set<Integer> vertexLabelIdSet) {
        Map<Integer, Map<Long, Map<String, Object>>> resultList = Maps.newHashMap();
        if (vertexLabelIdSet.isEmpty()) {
            return this.labelVertexList;
        } else {
            for (int vertexLabelId : vertexLabelIdSet) {
                Map<Long, Map<String, Object>> vertexProperties = this.labelVertexList.get(vertexLabelId);
                if (null != vertexProperties) {
                    resultList.put(vertexLabelId, vertexProperties);
                }
            }
        }
        return resultList;
    }

    public DefaultMemoryEdge getEdge(ElementId edgeId) {
        for (Map<Long, List<DefaultMemoryEdge>> outEdges : this.labelVertexOutEdgeList.values()) {
            for (List<DefaultMemoryEdge> edgeList : outEdges.values()) {
                for (DefaultMemoryEdge edge : edgeList) {
                    if (edge.edgeId.equals(edgeId)) {
                        return edge;
                    }
                }
            }
        }

        throw new IllegalArgumentException("Not found edge with id " + edgeId);
    }

    public List<DefaultMemoryEdge> getOutEdges(ElementId vertexId) {
        return this.labelVertexOutEdgeList.computeIfAbsent(vertexId.labelId(), k -> Maps.newHashMap())
                .computeIfAbsent(vertexId.id(), k -> Lists.newArrayList());
    }

    public List<DefaultMemoryEdge> getOutEdges(ElementId vertexId, Set<Integer> edgeLabelIdSet) {
        return this.labelVertexOutEdgeList.computeIfAbsent(vertexId.labelId(), k -> Maps.newHashMap())
                .computeIfAbsent(vertexId.id(), k -> Lists.newArrayList())
                .stream()
                .filter(v -> edgeLabelIdSet.contains(v.edgeId.labelId()))
                .collect(Collectors.toList());
    }

    public List<DefaultMemoryEdge> getInEdges(ElementId vertexId) {
        return this.labelVertexInEdgeList.computeIfAbsent(vertexId.labelId(), k -> Maps.newHashMap())
                .computeIfAbsent(vertexId.id(), k -> Lists.newArrayList());

    }

    public List<DefaultMemoryEdge> getInEdges(ElementId vertexId, Set<Integer> edgeLabelIdSet) {
        return this.labelVertexInEdgeList.computeIfAbsent(vertexId.labelId(), k -> Maps.newHashMap())
                .computeIfAbsent(vertexId.id(), k -> Lists.newArrayList())
                .stream()
                .filter(v -> edgeLabelIdSet.contains(v.edgeId.labelId()))
                .collect(Collectors.toList());

    }

    public List<DefaultMemoryEdge> scanEdges(Set<Integer> edgeLabelIdSet) {
        List<DefaultMemoryEdge> edgeList = Lists.newArrayList();
        if (edgeLabelIdSet.isEmpty()) {
            for (Map<Long, List<DefaultMemoryEdge>> outEdges : this.labelVertexOutEdgeList.values()) {
                for (List<DefaultMemoryEdge> edges : outEdges.values()) {
                    edgeList.addAll(edges);
                }
            }
        } else {
            for (Map<Long, List<DefaultMemoryEdge>> outEdges : this.labelVertexOutEdgeList.values()) {
                for (List<DefaultMemoryEdge> edges : outEdges.values()) {
                    edgeList.addAll(edges.stream().filter(e -> edgeLabelIdSet.contains(e.edgeId.labelId())).collect(Collectors.toList()));
                }
            }
        }
        return edgeList;
    }

    public static class DefaultMemoryEdge {
        private ElementId srcId;
        private ElementId destId;
        private ElementId edgeId;
        private Map<String, Object> properties;

        public DefaultMemoryEdge(ElementId srcId, ElementId destId, ElementId edgeId, Map<String, Object> properties) {
            this.srcId = srcId;
            this.destId = destId;
            this.edgeId = edgeId;
            this.properties = properties;
        }

        public ElementId getSrcId() {
            return srcId;
        }

        public ElementId getDestId() {
            return destId;
        }

        public ElementId getEdgeId() {
            return edgeId;
        }

        public Map<String, Object> getProperties() {
            return properties;
        }
    }
}
