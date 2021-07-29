package org.apache.tinkperpop.gremlin.groovy.custom;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;

public class AddColumnStep<S, E> extends MapStep<S, E> {
    private String srcColumn;
    private String dstColumn;

    public AddColumnStep(Traversal.Admin traversal, String srcColumn, String dstColumn) {
        super(traversal);
        this.srcColumn = srcColumn;
        this.dstColumn = dstColumn;
    }

    @Override
    protected E map(Traverser.Admin<S> traverser) {
        throw new UnsupportedOperationException("");
    }

    public String getSrcColumn() {
        return this.srcColumn;
    }

    public String getDstColumn() {
        return this.dstColumn;
    }
}
