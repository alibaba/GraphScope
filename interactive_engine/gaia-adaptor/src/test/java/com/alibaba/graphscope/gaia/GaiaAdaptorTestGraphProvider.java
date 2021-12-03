package com.alibaba.graphscope.gaia;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GaiaAdaptorTestGraphProvider extends AbstractGraphProvider {
    @Override
    public Map<String, Object> getBaseConfiguration(
            String graphName,
            Class<?> test,
            String testMethodName,
            LoadGraphWith.GraphData loadGraphWith) {
        Map config = new HashMap();
        config.put(Graph.GRAPH, RemoteTestGraph.class.getName());
        config.put(RemoteTestGraph.GRAPH_NAME, getModernGraphEndpoint());
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

    public String getModernGraphEndpoint() {
        try {
            InputStream inputStream =
                    GaiaAdaptorTestGraphProvider.class
                            .getClassLoader()
                            .getResourceAsStream("graph.endpoint");
            String endpoint = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            // remove invalid char
            return endpoint.replaceAll("[^A-Za-z0-9:]", "");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
