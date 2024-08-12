package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class LockFailedException extends GrootException {

    public LockFailedException(Throwable t) {
        super(Code.LOCK_FAILED, t);
    }

    public LockFailedException(String msg) {
        super(Code.LOCK_FAILED, msg);
    }

    public LockFailedException(String msg, Throwable t) {
        super(Code.LOCK_FAILED, msg, t);
    }

    public LockFailedException() {
        super(Code.LOCK_FAILED);
    }
}
