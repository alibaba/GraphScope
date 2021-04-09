package com.alibaba.maxgraph.v2.frontend.exception;

import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;

/**
 * When query is executed in maxgraph failed and throw this exception, query will try to be executed in tinkerpop
 */
public class ServerRetryGremlinException extends MaxGraphException {
    private static final long serialVersionUID = 7407571553275921396L;

    public ServerRetryGremlinException(String m) {
        super(m);
    }
}
