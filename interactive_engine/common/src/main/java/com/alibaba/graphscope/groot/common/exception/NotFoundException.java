package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class NotFoundException extends GrootException {
    public NotFoundException(Throwable t) {
        super(Code.NOT_FOUND, t);
    }

    public NotFoundException(String msg) {
        super(Code.NOT_FOUND, msg);
    }

    public NotFoundException(String msg, Throwable t) {
        super(Code.NOT_FOUND, msg, t);
    }

    public NotFoundException() {
        super(Code.NOT_FOUND);
    }
}
