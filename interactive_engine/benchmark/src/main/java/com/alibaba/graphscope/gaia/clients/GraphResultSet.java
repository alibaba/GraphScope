package com.alibaba.graphscope.gaia.clients;

public interface GraphResultSet {
    boolean hasNext();

    Object next();
}
