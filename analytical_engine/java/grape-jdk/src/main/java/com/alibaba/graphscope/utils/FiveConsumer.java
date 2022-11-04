package com.alibaba.graphscope.utils;

@FunctionalInterface
public interface FiveConsumer<T, U, V, W, X> {
    public void accept(T t, U u, V v, W w, X x);
}
