package com.alibaba.maxgraph.v2.frontend.compiler.exception;

public class RetryGremlinException extends Exception {
    public RetryGremlinException(Throwable t) {
        super(t);
    }

    public RetryGremlinException(String message, Throwable t) {
        super(message, t);
    }
}
