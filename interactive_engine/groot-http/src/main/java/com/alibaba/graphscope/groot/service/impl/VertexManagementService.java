package com.alibaba.graphscope.groot.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.groot.sdk.schema.Vertex;
import com.alibaba.graphscope.groot.service.models.Property;
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
        if (vertexRequests.size() == 1) {
            return addVertex(vertexRequests.get(0));
        } else {
            List<Vertex> vertices = new ArrayList<>();
            for (VertexRequest vertexRequest : vertexRequests) {
                Vertex vertex = DtoConverter.convertToVertex(vertexRequest);
                vertices.add(vertex);
            }
            return grootClient.addVertices(vertices);
        }
    }

    public long deleteVertex(String label, List<Property> properties) {
        Vertex vertex = DtoConverter.convertToVertex(label, properties);
        return grootClient.deleteVertex(vertex);
    }

    public long updateVertex(VertexRequest vertexRequest) {
        Vertex vertex = DtoConverter.convertToVertex(vertexRequest);
        return grootClient.updateVertex(vertex);
    }

}