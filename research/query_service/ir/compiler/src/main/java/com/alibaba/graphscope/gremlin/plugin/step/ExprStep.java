package com.alibaba.graphscope.gremlin.plugin.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;

import java.util.NoSuchElementException;

public class ExprStep<S, E> extends MapStep<S, E> {
    private String expr;
    private Type type;

    public ExprStep(Traversal.Admin traversal, String expr, Type type) {
        super(traversal);
        this.expr = expr;
        this.type = type;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        return null;
    }

    public String getExpr() {
        return expr;
    }

    public Type getType() {
        return type;
    }

    public enum Type {
        FILTER,
        PROJECTION
    }
}
