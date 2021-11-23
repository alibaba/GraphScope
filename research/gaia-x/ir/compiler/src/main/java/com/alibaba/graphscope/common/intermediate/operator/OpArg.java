package com.alibaba.graphscope.common.intermediate.operator;

import java.util.function.Function;

public class OpArg<T, R> {
    private T arg;

    private Function<T, R> transform;

    public OpArg(T arg, Function<T, R> transform) {
        this.arg = arg;
        this.transform = transform;
    }

    public R getArg() {
        return transform.apply(arg);
    }
}
