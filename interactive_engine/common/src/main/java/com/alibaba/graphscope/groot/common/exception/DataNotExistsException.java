package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class DataNotExistsException extends GrootException {
    public DataNotExistsException(Throwable t) {
        super(Code.DATA_NOT_EXISTS, t);
    }

    public DataNotExistsException(String msg) {
        super(Code.DATA_NOT_EXISTS, msg);
    }

    public DataNotExistsException(String msg, Throwable t) {
        super(Code.DATA_NOT_EXISTS, msg, t);
    }

    public DataNotExistsException() {
        super(Code.DATA_NOT_EXISTS);
    }
}
