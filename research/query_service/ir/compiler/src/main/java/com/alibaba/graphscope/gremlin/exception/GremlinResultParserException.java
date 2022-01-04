package com.alibaba.graphscope.gremlin.exception;

public class GremlinResultParserException extends IllegalArgumentException {
    public GremlinResultParserException(String error) {
        super(error);
    }
}
