package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class ValueTypeMismatchException extends GrootException {
    public ValueTypeMismatchException(Throwable t) {
        super(Code.VALUE_TYPE_MISMATCH, t);
    }

    public ValueTypeMismatchException(String msg) {
        super(Code.VALUE_TYPE_MISMATCH, msg);
    }

    public ValueTypeMismatchException(String msg, Throwable t) {
        super(Code.VALUE_TYPE_MISMATCH, msg, t);
    }

    public ValueTypeMismatchException() {
        super(Code.VALUE_TYPE_MISMATCH);
    }
}
