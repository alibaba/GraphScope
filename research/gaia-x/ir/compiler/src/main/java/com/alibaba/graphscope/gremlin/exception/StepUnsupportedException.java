package com.alibaba.graphscope.gremlin.exception;

import org.apache.tinkerpop.gremlin.process.traversal.Step;

public class StepUnsupportedException extends UnsupportedOperationException {
    public StepUnsupportedException(Class<? extends Step> type, String error) {
        super(String.format("step type {%s} is unsupported, cause is {%s}", type, error));
    }
}
