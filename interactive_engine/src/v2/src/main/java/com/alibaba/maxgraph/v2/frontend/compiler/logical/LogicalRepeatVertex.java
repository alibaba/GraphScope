package com.alibaba.maxgraph.v2.frontend.compiler.logical;

import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorRepeatFunction;

public class LogicalRepeatVertex extends LogicalUnaryVertex {
    public LogicalRepeatVertex(int id, ProcessorRepeatFunction processorFunction, LogicalVertex inputVertex) {
        super(id, processorFunction, false, inputVertex);
    }
}
