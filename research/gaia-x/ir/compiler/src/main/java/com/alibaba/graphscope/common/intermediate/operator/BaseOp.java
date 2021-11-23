package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class BaseOp<R> implements Supplier<R> {
    private Function<BaseOp, R> transformer;

    // set tag to store the intermediate result
    private Optional<OpArg> alias;

    public BaseOp(Function transformer) {
        this.transformer = transformer;
    }

    @Override
    public R get() {
        return this.transformer.apply(this);
    }

    public Optional<OpArg> getAlias() {
        return alias;
    }

    public void setAlias(OpArg alias) {
        this.alias = Optional.of(alias);
    }
}
