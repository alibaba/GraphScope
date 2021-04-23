package com.alibaba.maxgraph.v2.common.frontend.cache;

import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Map;

public class MaxGraphCache {
    private Map<ElementId, Vertex> vertexMap = Maps.newHashMap();
    private Map<ElementId, List<Edge>> outEdgeListMap = Maps.newHashMap();
    private Map<ElementId, List<Edge>> inEdgeListMap = Maps.newHashMap();

    public void addVertex(ElementId vertexId, Vertex vertex) {
        this.vertexMap.put(vertexId, vertex);
    }

    public void removeVertex(ElementId vertexId) {
        this.vertexMap.remove(vertexId);
    }

    public Vertex getVertex(ElementId vertexId) {
        return this.vertexMap.get(vertexId);
    }

    public void addOutEdgeList(ElementId vertexId, List<Edge> edgeList) {
        this.outEdgeListMap.put(vertexId, edgeList);
    }

    public void removeOutEdgeList(ElementId vertexId) {
        this.outEdgeListMap.remove(vertexId);
    }

    public List<Edge> getOutEdgeList(ElementId vertexId) {
        return this.outEdgeListMap.get(vertexId);
    }

    public void addInEdgeList(ElementId vertexId, List<Edge> edgeList) {
        this.inEdgeListMap.put(vertexId, edgeList);
    }

    public void removeInEdgeList(ElementId vertexId) {
        this.inEdgeListMap.remove(vertexId);
    }

    public List<Edge> getInEdgeList(ElementId vertexId) {
        return this.inEdgeListMap.get(vertexId);
    }
}
