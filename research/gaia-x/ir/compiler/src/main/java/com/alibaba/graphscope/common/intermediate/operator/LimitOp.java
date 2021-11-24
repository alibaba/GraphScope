package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class LimitOp extends InterOpBase {
    private Optional<OpArg> lower;

    private Optional<OpArg> upper;

    public LimitOp() {
        super();
        this.lower = Optional.empty();
        this.upper = Optional.empty();
    }

    public Optional<OpArg> getLower() {
        return lower;
    }

    public void setLower(OpArg lower) {
        this.lower = Optional.of(lower);
    }

    public Optional<OpArg> getUpper() {
        return upper;
    }

    public void setUpper(OpArg upper) {
        this.upper = Optional.of(upper);
    }
}
