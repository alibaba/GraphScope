package org.apache.tinkperpop.gremlin.groovy.custom;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Objects;

public final class ExprStep<S> extends MapStep<S, String> {

    private String expressionString;

    public ExprStep(final Traversal.Admin traversal, final String expressionString) {
        super(traversal);
        this.expressionString = expressionString;
    }

    public String getExpressionString() {
        return this.expressionString;
    }

    @Override
    protected String map(final Traverser.Admin<S> traverser) {
        return this.expressionString;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.expressionString);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.expressionString);
    }

}
