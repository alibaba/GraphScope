package com.alibaba.maxgraph.v2.common.frontend.api.exception;

import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;

/**
 * Realtime write data related exception
 */
public class GraphWriteDataException extends MaxGraphException {
    private static final long serialVersionUID = 4528133923696562486L;

    public GraphWriteDataException(String msg) {
        super(msg);
    }

    public GraphWriteDataException(String msg, Throwable t) {
        super(msg, t);
    }
}
