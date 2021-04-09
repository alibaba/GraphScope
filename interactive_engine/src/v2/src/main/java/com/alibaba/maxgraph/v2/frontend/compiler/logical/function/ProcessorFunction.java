package com.alibaba.maxgraph.v2.frontend.compiler.logical.function;

import com.alibaba.maxgraph.proto.v2.LogicalCompare;
import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.RangeLimit;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeConstants;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class ProcessorFunction {
    protected OperatorType operatorType;
    private Value.Builder argumentBuilder;
    private Set<Integer> usedLabelList = Sets.newHashSet();
    private RangeLimit.Builder rangeLimit;
    private List<LogicalCompare> logicalCompareList = Lists.newArrayList();

    public ProcessorFunction(OperatorType operatorType) {
        this(operatorType, null, null);
    }

    public ProcessorFunction(OperatorType operatorType, RangeLimit.Builder rangeLimit) {
        this(operatorType, null, rangeLimit);
    }

    public ProcessorFunction(OperatorType operatorType, Value.Builder argumentBuilder) {
        this(operatorType, argumentBuilder, null);
    }

    public ProcessorFunction(OperatorType operatorType, Value.Builder argumentBuilder, RangeLimit.Builder rangeLimit) {
        this.operatorType = checkNotNull(operatorType);
        this.argumentBuilder = argumentBuilder;
        this.rangeLimit = rangeLimit;
    }

    public OperatorType getOperatorType() {
        return operatorType;
    }

    public void resetOperatorType(OperatorType operatorType) {
        this.operatorType = operatorType;
    }

    public Value.Builder getArgumentBuilder() {
        return argumentBuilder;
    }

    public Set<Integer> getUsedLabelList() {
        return usedLabelList;
    }

    /**
     * Get real function used label list
     *
     * @return The function label used list
     */
    public Set<Integer> getFunctionUsedLabelList() {
        return usedLabelList
                .stream()
                .filter(v -> v <= TreeConstants.USER_LABEL_START && v > TreeConstants.MAGIC_LABEL_ID).collect(Collectors.toSet());
    }

    public RangeLimit.Builder getRangeLimit() {
        return rangeLimit;
    }

    public List<LogicalCompare> getLogicalCompareList() {
        return logicalCompareList;
    }

    public long estimatedOutputNumRecords(long inputNumRecords) {
        return inputNumRecords;
    }
}
