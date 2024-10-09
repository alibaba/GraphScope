package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class InvalidDataTypeException extends GrootException {
    public InvalidDataTypeException(Throwable t) {
        super(Code.INVALID_DATA_TYPE, t);
    }

    public InvalidDataTypeException(String msg) {
        super(Code.INVALID_DATA_TYPE, msg);
    }

    public InvalidDataTypeException(String msg, Throwable t) {
        super(Code.INVALID_DATA_TYPE, msg, t);
    }

    public InvalidDataTypeException() {
        super(Code.INVALID_DATA_TYPE);
    }
}
