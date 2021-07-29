package org.apache.tinkperpop.gremlin.groovy.custom;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;

public class SampleStep<S, E> extends MapStep<S, E> {
    private String format;

    public SampleStep(Traversal.Admin traversal, String format) {
        super(traversal);
        this.format = format;
    }

    @Override
    protected E map(Traverser.Admin<S> traverser) {
        throw new UnsupportedOperationException("");
    }

    public String getFormat() {
        return this.format;
    }
}
