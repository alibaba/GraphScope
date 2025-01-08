package com.alibaba.graphscope.groot.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.groot.sdk.schema.Edge;
import com.alibaba.graphscope.groot.sdk.schema.Vertex;
import com.alibaba.graphscope.groot.service.models.DeleteVertexRequest;
import com.alibaba.graphscope.groot.service.models.EdgeRequest;
import com.alibaba.graphscope.groot.service.models.VertexEdgeRequest;
import com.alibaba.graphscope.groot.service.models.VertexRequest;

@Service
public class VertexManagementService {

    private final GrootClient grootClient;

    @Autowired
    public VertexManagementService(GrootClient grootClient) {
        this.grootClient = grootClient;
    }

    public long addVertex(VertexRequest vertexRequest) {
        Vertex vertex = DtoConverter.convertToVertex(vertexRequest);
        return grootClient.addVertex(vertex);
    }

    public long addVertices(List<VertexRequest> vertexRequests) {
        List<Vertex> vertices = new ArrayList<>();
        for (VertexRequest vertexRequest : vertexRequests) {
            Vertex vertex = DtoConverter.convertToVertex(vertexRequest);
            vertices.add(vertex);
        }
        return grootClient.addVertices(vertices);
    }

    public long deleteVertex(DeleteVertexRequest deleteVertexRequest) {
        Vertex vertex = DtoConverter.convertToVertex(deleteVertexRequest);
        return grootClient.deleteVertex(vertex);
    }

    public long deleteVertices(List<DeleteVertexRequest> deleteVertexRequests) {
        List<Vertex> vertices = new ArrayList<>();
        for (DeleteVertexRequest deleteVertexRequest : deleteVertexRequests) {
            Vertex vertex = DtoConverter.convertToVertex(deleteVertexRequest);
            vertices.add(vertex);
        }
        return grootClient.deleteVertices(vertices);
    }

    public long updateVertex(VertexRequest vertexRequest) {
        Vertex vertex = DtoConverter.convertToVertex(vertexRequest);
        return grootClient.updateVertex(vertex);
    }

    public long updateVertices(List<VertexRequest> vertexRequests) {
        List<Vertex> vertices = new ArrayList<>();
        for (VertexRequest vertexRequest : vertexRequests) {
            Vertex vertex = DtoConverter.convertToVertex(vertexRequest);
            vertices.add(vertex);
        }
        return grootClient.updateVertices(vertices);
    }

    public long addVerticesAndEdges(VertexEdgeRequest vertexEdgeRequest) {
        List<Vertex> vertices = new ArrayList<>();
        for (VertexRequest vertexRequest : vertexEdgeRequest.getVertexRequest()) {
            Vertex vertex = DtoConverter.convertToVertex(vertexRequest);
            vertices.add(vertex);
        }
        List<Edge> edges = new ArrayList<>();
        for (EdgeRequest edgeRequest : vertexEdgeRequest.getEdgeRequest()) {
            Edge edge = DtoConverter.convertToEdge(edgeRequest);
            edges.add(edge);
        }
        return grootClient.addVerticesAndEdges(vertices, edges);
    }


    public boolean remoteFlush(long snapshotId) {
        return grootClient.remoteFlush(snapshotId);
    }
}