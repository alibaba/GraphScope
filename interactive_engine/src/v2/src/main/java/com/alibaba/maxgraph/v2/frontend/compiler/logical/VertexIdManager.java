package com.alibaba.maxgraph.v2.frontend.compiler.logical;

import java.util.concurrent.atomic.AtomicInteger;

public class VertexIdManager {
    private AtomicInteger id = new AtomicInteger(1);

    private VertexIdManager() {
    }

    public static VertexIdManager createVertexIdManager() {
        return new VertexIdManager();
    }

    public int getId() {
        return id.getAndIncrement();
    }
}
