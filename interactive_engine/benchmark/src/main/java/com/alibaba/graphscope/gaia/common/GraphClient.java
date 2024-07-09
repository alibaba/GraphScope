package com.alibaba.graphscope.gaia.common;

public interface GraphClient {
    GraphResultSet submit(String query);
    void close();
}
