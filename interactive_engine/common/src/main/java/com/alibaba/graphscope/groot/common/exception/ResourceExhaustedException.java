package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class ResourceExhaustedException extends GrootException {
    public ResourceExhaustedException(Throwable t) {
        super(Code.RESOURCE_EXHAUSTED, t);
    }

    public ResourceExhaustedException(String msg) {
        super(Code.RESOURCE_EXHAUSTED, msg);
    }

    public ResourceExhaustedException(String msg, Throwable t) {
        super(Code.RESOURCE_EXHAUSTED, msg, t);
    }

    public ResourceExhaustedException() {
        super(Code.RESOURCE_EXHAUSTED);
    }
}
