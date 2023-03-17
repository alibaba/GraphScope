/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.function.test;

import com.alibaba.graphscope.gremlin.integration.graph.RemoteTestGraph;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * {@link AbstractGraphProvider} is privided by gremlin to adapt to its test framework
 * {@link RemoteTestGraph} will be constructed by this provider
 */
public class RemoteTestGraphProvider extends AbstractGraphProvider {
    /**
     * how to construct your own graph
     *
     * @param graphName
     * @param test
     * @param testMethodName
     * @param loadGraphWith
     * @return
     */
    @Override
    public Map<String, Object> getBaseConfiguration(
            String graphName,
            Class<?> test,
            String testMethodName,
            LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {
            {
                put(Graph.GRAPH, RemoteTestGraph.class.getName());
                put(
                        RemoteTestGraph.GRAPH_NAME,
                        TestGlobalMeta.getGraphEndpoint(loadGraphWith.name()));
            }
        };
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
