package com.alibaba.graphscope.groot.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.groot.sdk.schema.Edge;
import com.alibaba.graphscope.groot.service.models.DeleteEdgeRequest;
import com.alibaba.graphscope.groot.service.models.EdgeRequest;

@Service
public class EdgeManagementService {
    private final GrootClient grootClient;

    @Autowired
    public EdgeManagementService(GrootClient grootClient) {
        this.grootClient = grootClient;
    }

    public long addEdge(EdgeRequest edgeRequest) {
        Edge edge = DtoConverter.convertToEdge(edgeRequest);
        return grootClient.addEdge(edge);
    }

    public long addEdges(List<EdgeRequest> edgeRequests) {
        if (edgeRequests.size() == 1) {
            return addEdge(edgeRequests.get(0));
        } else {
            List<Edge> edges = new ArrayList<>();
            for (EdgeRequest edgeRequest : edgeRequests) {
                Edge edge = DtoConverter.convertToEdge(edgeRequest);
                edges.add(edge);
            }
            return grootClient.addEdges(edges);
        }
    }

    public long deleteEdge(DeleteEdgeRequest deleteEdgeRequest) {
        Edge edge = DtoConverter.convertToEdge(deleteEdgeRequest);
        // TODO: deleteEdge will only delete the first edge that matches the given parameters
        // e.g., if we have multiple edges from v1 to v2 with the same edge label, only one of them will be deleted
        return grootClient.deleteEdge(edge);
    }

    public long deleteEdges(List<DeleteEdgeRequest> edgeRequests) {
        List<Edge> edges = new ArrayList<>();
        for (DeleteEdgeRequest edgeRequest : edgeRequests) {
            Edge edge = DtoConverter.convertToEdge(edgeRequest);
            edges.add(edge);
        }
        return grootClient.deleteEdges(edges);
    }

    public long updateEdge(EdgeRequest edgeRequest) {
        Edge edge = DtoConverter.convertToEdge(edgeRequest);
        // TODO: updateEdge will add a new edge even if it already exists
        return grootClient.updateEdge(edge);
    }

    public long updateEdges(List<EdgeRequest> edgeRequests) {
        List<Edge> edges = new ArrayList<>();
        for (EdgeRequest edgeRequest : edgeRequests) {
            Edge edge = DtoConverter.convertToEdge(edgeRequest);
            edges.add(edge);
        }
        return grootClient.updateEdges(edges);
    }
}
