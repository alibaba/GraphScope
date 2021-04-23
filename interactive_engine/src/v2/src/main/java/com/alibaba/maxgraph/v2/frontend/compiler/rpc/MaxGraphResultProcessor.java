package com.alibaba.maxgraph.v2.frontend.compiler.rpc;

import java.util.List;

public interface MaxGraphResultProcessor {
    void finish();

    long total();

    void process(List<Object> parseResponse);

    default Object transformResult(Object result) {
        return result;
    }
}
