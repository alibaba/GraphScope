package com.alibaba.graphscope.gremlin.exception;

import org.antlr.v4.runtime.tree.ParseTree;

public class UnsupportedAntlrException extends UnsupportedOperationException {
    public UnsupportedAntlrException(Class<? extends ParseTree> antlrCtx, String error) {
        super(String.format("antlr context {%s} parsing to traversal is unsupported, error is {}", antlrCtx, error));
    }
}
