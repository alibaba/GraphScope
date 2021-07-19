package org.apache.tinkperpop.gremlin.groovy.custom;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class ScatterStep<S, E> extends MapStep<S, E> implements TraversalParent, ByModulating {

    private String scatterName;
    private Traversal.Admin<S, E> scatterTraversal = null;

    public ScatterStep(final Traversal.Admin traversal, final String scatterName) {
        super(traversal);
        this.scatterName = scatterName;
    }

    public void modulateBy(final Traversal.Admin<?, ?> scatterTraversal) {
        this.scatterTraversal = this.integrateChild(scatterTraversal);
    }

    public String getScatterName() {
        return this.scatterName;
    }

    @Override
    protected E map(final Traverser.Admin<S> traverser) {
        return null;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.scatterName);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.scatterName);
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return null == this.scatterTraversal ? Collections.emptyList() : Collections.singletonList(this.scatterTraversal);
    }
}
