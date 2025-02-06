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
import com.alibaba.graphscope.groot.service.models.DeleteEdgeRequest;
import com.alibaba.graphscope.groot.service.models.EdgeRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

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
        // e.g., if we have multiple edges from v1 to v2 with the same edge label, only one of them
        // will be deleted
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
