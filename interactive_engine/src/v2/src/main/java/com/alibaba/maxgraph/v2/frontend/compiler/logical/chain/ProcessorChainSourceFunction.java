package com.alibaba.maxgraph.v2.frontend.compiler.logical.chain;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSourceVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;

import java.util.List;

public class ProcessorChainSourceFunction extends ProcessorFunction implements ProcessorChainFunction {
    private LogicalSourceVertex sourceVertex;
    private List<LogicalVertex> logicalVertexList;

    public ProcessorChainSourceFunction(LogicalSourceVertex sourceVertex, List<LogicalVertex> logicalVertexList) {
        super(OperatorType.SOURCE_CHAIN);
        this.sourceVertex = sourceVertex;
        this.logicalVertexList = logicalVertexList;
    }

    @Override
    public List<LogicalVertex> getChainVertexList() {
        return logicalVertexList;
    }
}
