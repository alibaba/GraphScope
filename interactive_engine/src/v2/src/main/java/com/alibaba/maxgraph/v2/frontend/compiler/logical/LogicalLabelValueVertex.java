package com.alibaba.maxgraph.v2.frontend.compiler.logical;


import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorLabelValueFunction;

public class LogicalLabelValueVertex extends LogicalUnaryVertex {
    private LogicalVertex labelValueVertex;
    private int labelId;

    public LogicalLabelValueVertex(int id,
                                   LogicalVertex labelValueVertex,
                                   int labelId,
                                   LogicalVertex inputVertex) {
        super(id, new ProcessorFunction(OperatorType.LABEL_VALUE), inputVertex);
        this.labelValueVertex = labelValueVertex;
        this.labelId = labelId;
    }

    @Override
    public ProcessorFunction getProcessorFunction() {
        return new ProcessorLabelValueFunction(this.labelId, this.labelValueVertex);
    }
}
