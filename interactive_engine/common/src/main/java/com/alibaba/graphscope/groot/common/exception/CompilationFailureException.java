package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class CompilationFailureException extends GrootException {
    public CompilationFailureException(Throwable t) {
        super(Code.COMPILATION_FAILURE, t);
    }

    public CompilationFailureException(String msg) {
        super(Code.COMPILATION_FAILURE, msg);
    }

    public CompilationFailureException(String msg, Throwable t) {
        super(Code.COMPILATION_FAILURE, msg, t);
    }

    public CompilationFailureException() {
        super(Code.COMPILATION_FAILURE);
    }
}
