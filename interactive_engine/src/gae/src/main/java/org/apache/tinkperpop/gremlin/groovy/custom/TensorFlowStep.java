package org.apache.tinkperpop.gremlin.groovy.custom;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;

public class TensorFlowStep<S, E> extends MapStep<S, E> {
    public TensorFlowStep(Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected E map(Traverser.Admin<S> traverser) {
        throw new UnsupportedOperationException("");
    }
}
