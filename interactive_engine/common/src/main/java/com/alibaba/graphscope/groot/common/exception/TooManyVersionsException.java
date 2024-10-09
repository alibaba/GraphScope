package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class TooManyVersionsException extends GrootException {
    public TooManyVersionsException(Throwable t) {
        super(Code.TOO_MANY_VERSIONS, t);
    }

    public TooManyVersionsException(String msg) {
        super(Code.TOO_MANY_VERSIONS, msg);
    }

    public TooManyVersionsException(String msg, Throwable t) {
        super(Code.TOO_MANY_VERSIONS, msg, t);
    }

    public TooManyVersionsException() {
        super(Code.TOO_MANY_VERSIONS);
    }
}
