package com.alibaba.graphscope.common.exception;

public class OpArgIllegalException extends IllegalArgumentException {
    public enum Cause {
        UNSUPPORTED_TYPE,
        INVALID_TYPE
    }

    public OpArgIllegalException(Cause cause, String error) {
        super(String.format("cause is {%s} error is {%s}", cause, error));
    }
}
