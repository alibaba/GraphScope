package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class SelectOp extends InterOpBase {
    public SelectOp() {
        super();
        this.predicate = Optional.empty();
    }

    private Optional<OpArg> predicate;

    public Optional<OpArg> getPredicate() {
        return predicate;
    }

    public void setPredicate(OpArg predicate) {
        this.predicate = Optional.of(predicate);
    }
}
