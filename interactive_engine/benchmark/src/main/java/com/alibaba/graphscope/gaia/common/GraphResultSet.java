package com.alibaba.graphscope.gaia.common;

public interface GraphResultSet {
    boolean hasNext();

    Object next();

    Object get();
}
