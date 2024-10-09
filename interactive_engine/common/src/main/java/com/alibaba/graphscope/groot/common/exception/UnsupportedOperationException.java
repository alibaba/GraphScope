package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class UnsupportedOperationException extends GrootException {
    public UnsupportedOperationException(Throwable t) {
        super(Code.UNSUPPORTED_OPERATION, t);
    }

    public UnsupportedOperationException(String msg) {
        super(Code.UNSUPPORTED_OPERATION, msg);
    }

    public UnsupportedOperationException(String msg, Throwable t) {
        super(Code.UNSUPPORTED_OPERATION, msg, t);
    }

    public UnsupportedOperationException() {
        super(Code.UNSUPPORTED_OPERATION);
    }
}
