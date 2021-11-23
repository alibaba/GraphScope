package com.alibaba.graphscope.common.intermediate.operator;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OpArg<T, R> {
    private T arg;

    private List<T> args;

    private Function<T, R> transform;

    public OpArg(T arg, Function<T, R> transform) {
        this.arg = arg;
        this.transform = transform;
    }

    public OpArg(List<T> args, Function<T, R> transform) {
        this.args = args;
        this.transform = transform;
    }

    public R getArg() {
        return transform.apply(arg);
    }

    public List<R> getArgs() {
        return args.stream().map(k -> transform.apply(k)).collect(Collectors.toList());
    }
}
