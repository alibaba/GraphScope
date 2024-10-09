package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class QueryFailedException extends GrootException {
    public QueryFailedException(Throwable t) {
        super(Code.QUERY_FAILED, t);
    }

    public QueryFailedException(String msg) {
        super(Code.QUERY_FAILED, msg);
    }

    public QueryFailedException(String msg, Throwable t) {
        super(Code.QUERY_FAILED, msg, t);
    }

    public QueryFailedException() {
        super(Code.QUERY_FAILED);
    }
}
