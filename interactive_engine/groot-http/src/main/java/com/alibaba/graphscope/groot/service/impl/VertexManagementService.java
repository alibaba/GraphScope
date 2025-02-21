/*
 * Copyright 2025 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.groot.service.impl;

import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.groot.sdk.schema.Edge;
import com.alibaba.graphscope.groot.sdk.schema.Vertex;
import com.alibaba.graphscope.groot.service.models.DeleteVertexRequest;
import com.alibaba.graphscope.groot.service.models.EdgeRequest;
import com.alibaba.graphscope.groot.service.models.VertexEdgeRequest;
import com.alibaba.graphscope.groot.service.models.VertexRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

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

    public long updateVerticesAndEdges(VertexEdgeRequest vertexEdgeRequest) {
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
        return grootClient.updateVerticesAndEdges(vertices, edges);
    }
}
