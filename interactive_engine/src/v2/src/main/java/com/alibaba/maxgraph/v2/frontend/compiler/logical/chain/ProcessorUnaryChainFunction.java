package com.alibaba.maxgraph.v2.frontend.compiler.logical.chain;


import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;

import java.util.List;

public class ProcessorUnaryChainFunction extends ProcessorFunction implements ProcessorChainFunction {
    private List<LogicalVertex> chainVertexList;

    public ProcessorUnaryChainFunction(List<LogicalVertex> chainVertexList) {
        super(OperatorType.UNARY_CHAIN);
        this.chainVertexList = chainVertexList;
    }

    @Override
    public List<LogicalVertex> getChainVertexList() {
        return this.chainVertexList;
    }
}
