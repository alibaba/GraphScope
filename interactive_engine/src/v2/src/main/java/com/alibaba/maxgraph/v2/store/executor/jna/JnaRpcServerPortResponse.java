package com.alibaba.maxgraph.v2.store.executor.jna;

import com.sun.jna.Structure;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class JnaRpcServerPortResponse extends Structure implements Closeable {
    public int storeQueryPort;
    public int queryExecutePort;
    public int queryManagePort;

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("storeQueryPort", "queryExecutePort", "queryManagePort");
    }

    @Override
    public void close() throws IOException {

    }
}
