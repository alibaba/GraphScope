package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class PermissionDeniedException extends GrootException {
    public PermissionDeniedException(Throwable t) {
        super(Code.PERMISSION_DENIED, t);
    }

    public PermissionDeniedException(String msg) {
        super(Code.PERMISSION_DENIED, msg);
    }

    public PermissionDeniedException(String msg, Throwable t) {
        super(Code.PERMISSION_DENIED, msg, t);
    }

    public PermissionDeniedException() {
        super(Code.PERMISSION_DENIED);
    }
}
