package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class InvalidArgumentException extends GrootException {

    public InvalidArgumentException(Throwable t) {
        super(Code.INVALID_ARGUMENT, t);
    }

    public InvalidArgumentException(String msg) {
        super(Code.INVALID_ARGUMENT, msg);
    }

    public InvalidArgumentException(String msg, Throwable t) {
        super(Code.INVALID_ARGUMENT, msg, t);
    }

    public InvalidArgumentException() {
        super(Code.INVALID_ARGUMENT);
    }
}
