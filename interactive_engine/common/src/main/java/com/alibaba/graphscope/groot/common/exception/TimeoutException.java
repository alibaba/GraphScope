package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class TimeoutException extends GrootException {
    public TimeoutException(Throwable t) {
        super(Code.TIMEOUT, t);
    }

    public TimeoutException(String msg) {
        super(Code.TIMEOUT, msg);
    }

    public TimeoutException(String msg, Throwable t) {
        super(Code.TIMEOUT, msg, t);
    }

    public TimeoutException() {
        super(Code.TIMEOUT);
    }
}
