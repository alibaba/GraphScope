package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class UnimplementedException extends GrootException {
    public UnimplementedException(Throwable t) {
        super(Code.UNIMPLEMENTED, t);
    }

    public UnimplementedException(String msg) {
        super(Code.UNIMPLEMENTED, msg);
    }

    public UnimplementedException(String msg, Throwable t) {
        super(Code.UNIMPLEMENTED, msg, t);
    }

    public UnimplementedException() {
        super(Code.UNIMPLEMENTED);
    }
}
