package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class GroupOp extends InterOpBase {
    // List of Pair<FfiVariable, FfiAlias>
    private Optional<OpArg> groupByKeys;

    // List<ArgAggFn>
    private Optional<OpArg> groupByValues;

    public GroupOp() {
        super();
        groupByKeys = Optional.empty();
        groupByValues = Optional.empty();
    }

    public Optional<OpArg> getGroupByKeys() {
        return groupByKeys;
    }

    public Optional<OpArg> getGroupByValues() {
        return groupByValues;
    }

    public void setGroupByKeys(OpArg groupByKeys) {
        this.groupByKeys = Optional.of(groupByKeys);
    }

    public void setGroupByValues(OpArg groupByValues) {
        this.groupByValues = Optional.of(groupByValues);
    }
}
