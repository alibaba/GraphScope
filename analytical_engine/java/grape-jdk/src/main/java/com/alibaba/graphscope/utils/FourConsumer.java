package com.alibaba.graphscope.utils;

@FunctionalInterface
public interface FourConsumer<T, U, V, W> {
    public void accept(T t, U u, V v, W w);
}
