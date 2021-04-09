package com.alibaba.maxgraph.v2.common;

public interface CompletionCallback<T> {
    void onCompleted(T res);

    void onError(Throwable t);
}
