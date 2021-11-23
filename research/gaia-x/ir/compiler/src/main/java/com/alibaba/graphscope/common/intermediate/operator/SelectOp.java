package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;
import java.util.function.Function;

public class SelectOp extends BaseOp {
    public SelectOp(Function transformer) {
        super(transformer);
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
