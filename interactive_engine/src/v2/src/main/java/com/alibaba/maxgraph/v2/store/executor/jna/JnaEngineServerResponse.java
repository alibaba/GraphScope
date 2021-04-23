package com.alibaba.maxgraph.v2.store.executor.jna;

import com.sun.jna.Structure;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;

public class JnaEngineServerResponse extends Structure implements Closeable {

    public int errCode;
    public String errMsg;
    public String address;

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("errCode", "errMsg", "address");
    }

    @Override
    public void close() {
        setAutoSynch(false);
        ExecutorLibrary.INSTANCE.dropJnaServerResponse(this);
    }
}
