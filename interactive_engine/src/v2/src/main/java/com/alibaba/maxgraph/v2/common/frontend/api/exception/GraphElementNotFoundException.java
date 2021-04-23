package com.alibaba.maxgraph.v2.common.frontend.api.exception;

import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;

/**
 * Get schema element related exception
 */
public class GraphElementNotFoundException extends MaxGraphException {
    private static final long serialVersionUID = 4762154639895425883L;

    public GraphElementNotFoundException(String msg) {
        super(msg);
    }
}
