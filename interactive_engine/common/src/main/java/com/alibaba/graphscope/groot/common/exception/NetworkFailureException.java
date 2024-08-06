package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class NetworkFailureException extends GrootException {
    public NetworkFailureException(Throwable t) {
        super(Code.NETWORK_FAILURE, t);
    }

    public NetworkFailureException(String msg) {
        super(Code.NETWORK_FAILURE, msg);
    }

    public NetworkFailureException(String msg, Throwable t) {
        super(Code.NETWORK_FAILURE, msg, t);
    }

    public NetworkFailureException() {
        super(Code.NETWORK_FAILURE);
    }
}
