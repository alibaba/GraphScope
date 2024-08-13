package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class CancelledException extends GrootException {
    public CancelledException(String msg) {
        super(Code.CANCELLED, msg);
    }

    public CancelledException(String msg, Throwable t) {
        super(Code.CANCELLED, msg, t);
    }

    public CancelledException() {
        super(Code.CANCELLED);
    }

    public CancelledException(Throwable t) {
        super(Code.CANCELLED, t);
    }
}
