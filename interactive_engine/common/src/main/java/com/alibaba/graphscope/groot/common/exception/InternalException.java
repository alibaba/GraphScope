package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class InternalException extends GrootException {
    public InternalException(Throwable t) {
        super(Code.INTERNAL, t);
    }

    public InternalException(String msg) {
        super(Code.INTERNAL, msg);
    }

    public InternalException(String msg, Throwable t) {
        super(Code.INTERNAL, msg, t);
    }

    public InternalException() {
        super(Code.INTERNAL);
    }
}
