package com.alibaba.graphscope.gremlin.antlr4;

import org.apache.tinkerpop.gremlin.process.traversal.P;

public class ExprP extends P<String> {
    public ExprP(String expr) {
        super(null, expr);
    }
}
