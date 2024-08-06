package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class ExternalStorageErrorException extends GrootException {
    public ExternalStorageErrorException(Throwable t) {
        super(Code.EXTERNAL_STORAGE_ERROR, t);
    }

    public ExternalStorageErrorException(String msg) {
        super(Code.EXTERNAL_STORAGE_ERROR, msg);
    }

    public ExternalStorageErrorException(String msg, Throwable t) {
        super(Code.EXTERNAL_STORAGE_ERROR, msg, t);
    }

    public ExternalStorageErrorException() {
        super(Code.EXTERNAL_STORAGE_ERROR);
    }
}
