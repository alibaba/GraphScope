package com.alibaba.maxgraph.v2.common.frontend.api.exception;

import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;

/**
 * Create schema related exception
 */
public class GraphCreateSchemaException extends MaxGraphException {
    private static final long serialVersionUID = -8780742469260074671L;

    public GraphCreateSchemaException(String msg) {
        super(msg);
    }

    public GraphCreateSchemaException(String msg, Throwable t) {
        super(msg, t);
    }
}
