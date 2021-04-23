package com.alibaba.maxgraph.v2.common.frontend.api.exception;

import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;

/**
 * Get property definition from schema related exception
 */
public class GraphPropertyNotFoundException extends MaxGraphException {
    private static final long serialVersionUID = -2338594695214013072L;

    public GraphPropertyNotFoundException(String msg) {
        super(msg);
    }
}
