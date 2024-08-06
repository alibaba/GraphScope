package com.alibaba.graphscope.gaia.clients;

public class GraphSystem {
    private String name;
    private GraphClient client;

    public GraphSystem(String name, GraphClient client) {
        this.name = name;
        this.client = client;
    }

    public String getName() {
        return name;
    }

    public GraphClient getClient() {
        return client;
    }
}
