package com.alibaba.maxgraph.v2.common.frontend.api.exception;

import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;

/**
 * Execute gremlin query related exception
 */
public class GraphQueryDataException extends MaxGraphException {
    private static final long serialVersionUID = 6421520649781463393L;

    public GraphQueryDataException(String message) {
        super(message);
    }

    public GraphQueryDataException(String message, Throwable t) {
        super(message, t);
    }
}
