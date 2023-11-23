package com.alibaba.graphscope.groot.sdk.api;

import com.alibaba.graphscope.groot.sdk.schema.Edge;
import com.alibaba.graphscope.groot.sdk.schema.Vertex;

import java.util.List;

public interface Writer {
    long addVertex(Vertex vertex);

    long addVertices(List<Vertex> vertices);

    long updateVertex(Vertex vertex);

    long updateVertices(List<Vertex> vertices);

    long clearVertexProperty(Vertex vertex);

    long clearVertexProperties(List<Vertex> vertices);

    long deleteVertex(Vertex vertex);

    long deleteVertices(List<Vertex> vertices);

    long addEdge(Edge edge);

    long addEdges(List<Edge> edges);

    long updateEdge(Edge edge);

    long updateEdges(List<Edge> edges);

    long clearEdgeProperty(Edge edge);

    long clearEdgeProperties(List<Edge> edges);

    long deleteEdge(Edge edge);

    long deleteEdges(List<Edge> edges);

    long addVerticesAndEdges(List<Vertex> vertices, List<Edge> edges);

    long updateVerticesAndEdges(List<Vertex> vertices, List<Edge> edges);

    long deleteVerticesAndEdges(List<Vertex> vertices, List<Edge> edges);
}
