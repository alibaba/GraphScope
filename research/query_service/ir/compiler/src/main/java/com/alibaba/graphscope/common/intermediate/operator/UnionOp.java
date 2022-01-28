package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class UnionOp extends InterOpBase {
    // List<InterOpCollection>
    private Optional<OpArg> subOpCollectionList;

    // List<Integer>
    private Optional<OpArg> parentIdList;

    public UnionOp() {
        super();
        this.subOpCollectionList = Optional.empty();
        this.parentIdList = Optional.empty();
    }

    public Optional<OpArg> getSubOpCollectionList() {
        return subOpCollectionList;
    }

    public Optional<OpArg> getParentIdList() {
        return parentIdList;
    }

    public void setSubOpCollectionList(OpArg subOpCollectionList) {
        this.subOpCollectionList = Optional.of(subOpCollectionList);
    }

    public void setParentIdList(OpArg parentIdList) {
        this.parentIdList = Optional.of(parentIdList);
    }
}
