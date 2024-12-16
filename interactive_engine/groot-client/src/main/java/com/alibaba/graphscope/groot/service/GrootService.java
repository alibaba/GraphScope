/**
 * Copyright 2024 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.groot.sdk.schema.Vertex;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

@Service
public class GrootService {
    private final GrootClient grootClient;
    private final Client gremlinClient;
    
    @Autowired
    public GrootService(GrootClient grootClient, Client gremlinClient) {
        this.grootClient = grootClient;
        this.gremlinClient = gremlinClient;
    }

   public long addVertex(Vertex vertex) {
        return grootClient.addVertex(vertex);
    }

    public long addVertices(List<Vertex> vertices) {
        return grootClient.addVertices(vertices);
    }

    public CompletableFuture<List<Result>> executeGremlinQuery(String query) {
        ResultSet resultSet = gremlinClient.submit(query);
        return resultSet.all();
    }
}
