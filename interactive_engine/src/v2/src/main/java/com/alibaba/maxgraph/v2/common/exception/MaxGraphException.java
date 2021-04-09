package com.alibaba.maxgraph.v2.common.exception;

public class MaxGraphException extends RuntimeException {

    public MaxGraphException(Throwable t) {
        super(t);
    }

    public MaxGraphException(String msg) {
        super(msg);
    }

    public MaxGraphException(String msg, Throwable t) {
        super(msg, t);
    }
}
