package com.alibaba.maxgraph.v2.frontend.compiler.logical.chain;

import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;

import java.util.List;

public interface ProcessorChainFunction {
    List<LogicalVertex> getChainVertexList();
}
