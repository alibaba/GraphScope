package com.alibaba.graphscope.gaia.clients;

public interface GraphClient {
    GraphResultSet submit(String query);

    void close();
}
