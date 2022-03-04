package com.alibaba.graphscope.gremlin.plugin.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;

import java.util.NoSuchElementException;

public class ExprStep<S, E> extends MapStep<S, E> {
    private String expr;

    public ExprStep(Traversal.Admin traversal, String expr) {
        super(traversal);
        this.expr = expr;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        return null;
    }

    public String getExpr() {
        return expr;
    }
}
