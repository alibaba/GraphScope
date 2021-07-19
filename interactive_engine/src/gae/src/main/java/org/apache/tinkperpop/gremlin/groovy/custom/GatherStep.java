
package org.apache.tinkperpop.gremlin.groovy.custom;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Objects;


public final class GatherStep<S> extends MapStep<S, Vertex> {

    final private String gatherName;
    final private Operator op;

    public GatherStep(final Traversal.Admin traversal, final String gatherName, final Operator op) {
        super(traversal);
        this.gatherName = gatherName;
        this.op = op;
    }

    public String getGatherName() {
        return this.gatherName;
    }
    public Operator getOp() {
        return this.op;
    }

    @Override
    protected Vertex map(final Traverser.Admin<S> traverser) {
        return null;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.gatherName, this.op);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.gatherName) ^ Objects.hashCode(this.op);
    }

}
