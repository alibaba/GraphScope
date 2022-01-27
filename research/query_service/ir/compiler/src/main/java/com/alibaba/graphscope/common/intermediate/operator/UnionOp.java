package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class UnionOp extends InterOpBase {
    // List<InterOpCollection>
    private Optional<OpArg> subOpCollectionList;

    // List<Integer>
    private Optional<OpArg> subRootIdList;

    public UnionOp() {
        super();
        this.subOpCollectionList = Optional.empty();
        this.subRootIdList = Optional.empty();
    }

    public Optional<OpArg> getSubOpCollectionList() {
        return subOpCollectionList;
    }

    public Optional<OpArg> getSubRootIdList() {
        return subRootIdList;
    }

    public void setSubOpCollectionList(OpArg subOpCollectionList) {
        this.subOpCollectionList = Optional.of(subOpCollectionList);
    }

    public void setSubRootIdList(OpArg subRootIdList) {
        this.subRootIdList = Optional.of(subRootIdList);
    }
}
