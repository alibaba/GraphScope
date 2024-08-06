package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class AlreadyExistsException extends GrootException {
    public AlreadyExistsException(Throwable t) {
        super(Code.ALREADY_EXISTS, t);
    }

    public AlreadyExistsException(String msg) {
        super(Code.ALREADY_EXISTS, msg);
    }

    public AlreadyExistsException(String msg, Throwable t) {
        super(Code.ALREADY_EXISTS, msg, t);
    }

    public AlreadyExistsException() {
        super(Code.ALREADY_EXISTS);
    }
}
