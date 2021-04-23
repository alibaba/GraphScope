package com.alibaba.maxgraph.v2.frontend.compiler.executor;

import com.google.protobuf.ByteString;

import java.util.List;

public interface QueryExecuteListener {
    void onReceive(List<ByteString> byteStrings);

    void onError(Throwable t);

    void onCompleted();
}
