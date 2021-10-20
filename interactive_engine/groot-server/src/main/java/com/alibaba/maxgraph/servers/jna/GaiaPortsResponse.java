package com.alibaba.maxgraph.servers.jna;

import com.sun.jna.Structure;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;

public class GaiaPortsResponse extends Structure implements Closeable {
    public boolean success;
    public String errMsg;
    public int enginePort;
    public int rpcPort;

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("success", "errMsg", "enginePort", "rpcPort");
    }

    @Override
    public void close() {
        setAutoSynch(false);
        GaiaLibrary.INSTANCE.dropEnginePortsResponse(this);
    }
}
