package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class ApplyOp extends InterOpBase {
    // InterOpCollection
    private Optional<OpArg> subOpCollection;

    // Integer
    private Optional<OpArg> subRootId;

    // FfiJoinKind
    private Optional<OpArg> joinKind;

    public ApplyOp() {
        super();
        subOpCollection = Optional.empty();
        subRootId = Optional.empty();
        joinKind = Optional.empty();
    }

    public Optional<OpArg> getSubOpCollection() {
        return subOpCollection;
    }

    public Optional<OpArg> getSubRootId() {
        return subRootId;
    }

    public Optional<OpArg> getJoinKind() {
        return joinKind;
    }

    public void setSubOpCollection(OpArg subOpCollection) {
        this.subOpCollection = Optional.of(subOpCollection);
    }

    public void setSubRootId(OpArg subRootId) {
        this.subRootId = Optional.of(subRootId);
    }

    public void setJoinKind(OpArg joinKind) {
        this.joinKind = Optional.of(joinKind);
    }
}
