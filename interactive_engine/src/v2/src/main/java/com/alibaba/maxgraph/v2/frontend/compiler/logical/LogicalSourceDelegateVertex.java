package com.alibaba.maxgraph.v2.frontend.compiler.logical;

import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;

public class LogicalSourceDelegateVertex extends LogicalSourceVertex {
    private LogicalVertex delegateVertex;
    public LogicalSourceDelegateVertex(LogicalVertex delegateVertex) {
        super(delegateVertex.getId(), null, delegateVertex.isPropLocalFlag());
        this.delegateVertex = delegateVertex;
    }

    public LogicalVertex getDelegateVertex() {
        return delegateVertex;
    }

    public ProcessorFunction getProcessorFunction() {
        return delegateVertex.processorFunction;
    }
}
