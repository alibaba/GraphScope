package com.alibaba.maxgraph.v2.frontend.compiler.logical.function;


import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.RangeLimit;
import com.alibaba.maxgraph.proto.v2.Value;

public class ProcessorSourceFunction extends ProcessorFunction {

    public ProcessorSourceFunction(OperatorType operatorType, RangeLimit.Builder rangeLimit) {
        this(operatorType, null, rangeLimit);
    }

    public ProcessorSourceFunction(OperatorType operatorType, Value.Builder argumentBuilder, RangeLimit.Builder rangeLimit) {
        super(operatorType, argumentBuilder, rangeLimit);
    }

    public void resetOperatorType(OperatorType operatorType) {
        this.operatorType = operatorType;
    }
}
