package com.alibaba.maxgraph.v2.frontend.compiler.logical.function;

import com.alibaba.maxgraph.proto.v2.LogicalCompare;
import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.google.common.collect.Lists;

import java.util.List;

public class ProcessorFilterFunction extends ProcessorFunction implements CompareFunction {
    private List<LogicalCompare> logicalCompareList = Lists.newArrayList();

    public ProcessorFilterFunction(OperatorType operatorType, Value.Builder argumentBuilder) {
        super(operatorType, argumentBuilder, null);
    }

    public ProcessorFilterFunction(OperatorType operatorType) {
        this(operatorType, null);
    }

    @Override
    public List<LogicalCompare> getLogicalCompareList() {
        return logicalCompareList;
    }
}
