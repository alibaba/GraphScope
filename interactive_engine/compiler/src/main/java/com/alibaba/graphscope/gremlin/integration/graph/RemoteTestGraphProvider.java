/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.gremlin.integration.graph;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RemoteTestGraphProvider extends AbstractGraphProvider {
    private static String GREMLIN_ENDPOINT = "gremlin.endpoint";
    private static String DEFAULT_VALUE = "localhost:8182";
    private String gremlinEndpoint;

    public RemoteTestGraphProvider() {
        String property = System.getProperty(GREMLIN_ENDPOINT);
        gremlinEndpoint = (property != null) ? property : DEFAULT_VALUE;
    }

    @Override
    public Map<String, Object> getBaseConfiguration(
            String graphName,
            Class<?> test,
            String testMethodName,
            LoadGraphWith.GraphData loadGraphWith) {
        Map config = new HashMap();
        config.put(Graph.GRAPH, RemoteTestGraph.class.getName());
        config.put(RemoteTestGraph.GRAPH_NAME, gremlinEndpoint);
        return config;
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        if (graph != null) graph.close();
    }

    @Override
    public Set<Class> getImplementations() {
        return null;
    }

    @Override
    public void loadGraphData(
            final Graph graph,
            final LoadGraphWith loadGraphWith,
            final Class testClass,
            final String testName) {
        // do nothing
    }
}
