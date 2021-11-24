package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public abstract class InterOpBase {
    // set tag to store the intermediate result
    private Optional<OpArg> alias;

    public InterOpBase() {
        this.alias = Optional.empty();
    }

    public Optional<OpArg> getAlias() {
        return alias;
    }

    public void setAlias(OpArg alias) {
        this.alias = Optional.of(alias);
    }
}
