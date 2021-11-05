package com.alibaba.graphscope.common.jna;

public interface IntEnum<T> {
    int getInt();

    T getEnum(int i);
}
