package com.alibaba.graphscope.gremlin.exception;

public class InvalidGremlinScriptException extends IllegalArgumentException {
    public InvalidGremlinScriptException(String error) {
        super(error);
    }
}
