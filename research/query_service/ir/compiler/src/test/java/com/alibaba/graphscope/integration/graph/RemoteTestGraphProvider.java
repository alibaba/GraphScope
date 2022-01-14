package com.alibaba.graphscope.integration.graph;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RemoteTestGraphProvider extends AbstractGraphProvider {
    public static String MODERN_GRAPH_ENDPOINT = "localhost:8182";

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        Map config = new HashMap();
        config.put(Graph.GRAPH, RemoteTestGraph.class.getName());
        config.put(RemoteTestGraph.GRAPH_NAME, MODERN_GRAPH_ENDPOINT);
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
    public void loadGraphData(final Graph graph, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {
        // do nothing
    }
}