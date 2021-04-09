package com.alibaba.maxgraph.v2.frontend.compiler.logical.function;


import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;

public class ProcessorLabelValueFunction extends ProcessorFunction {
    private int labelId;
    private LogicalVertex labelValueVertex;
    private boolean requireLabelFlag = false;

    public ProcessorLabelValueFunction(int labelId, LogicalVertex labelValueVertex) {
        super(OperatorType.LABEL_VALUE, Value.newBuilder().setIntValue(labelId));
        this.labelValueVertex = labelValueVertex;
        this.labelId = labelId;
    }

    public void setRequireLabelFlag(boolean requireLabelFlag) {
        this.requireLabelFlag = requireLabelFlag;
    }

    public LogicalVertex getLabelValueVertex() {
        return labelValueVertex;
    }

    public int getLabelId() {
        return this.labelId;
    }

    public boolean getRequireLabelFlag() {
        return this.requireLabelFlag;
    }
}
