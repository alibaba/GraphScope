package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class InvalidOperationException extends GrootException {
    public InvalidOperationException(Throwable t) {
        super(Code.INVALID_OPERATION, t);
    }

    public InvalidOperationException(String msg) {
        super(Code.INVALID_OPERATION, msg);
    }

    public InvalidOperationException(String msg, Throwable t) {
        super(Code.INVALID_OPERATION, msg, t);
    }

    public InvalidOperationException() {
        super(Code.INVALID_OPERATION);
    }
}
