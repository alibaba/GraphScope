package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class IllegalStateException extends GrootException {
    public IllegalStateException(Throwable t) {
        super(Code.ILLEGAL_STATE, t);
    }

    public IllegalStateException(String msg) {
        super(Code.ILLEGAL_STATE, msg);
    }

    public IllegalStateException(String msg, Throwable t) {
        super(Code.ILLEGAL_STATE, msg, t);
    }

    public IllegalStateException() {
        super(Code.ILLEGAL_STATE);
    }
}
