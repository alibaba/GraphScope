package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class RealtimeWriteFailedException extends GrootException {
    public RealtimeWriteFailedException(Throwable t) {
        super(Code.REALTIME_WRITE_FAILED, t);
    }

    public RealtimeWriteFailedException(String msg) {
        super(Code.REALTIME_WRITE_FAILED, msg);
    }

    public RealtimeWriteFailedException(String msg, Throwable t) {
        super(Code.REALTIME_WRITE_FAILED, msg, t);
    }

    public RealtimeWriteFailedException() {
        super(Code.REALTIME_WRITE_FAILED);
    }
}
