package org.apache.tinkperpop.gremlin.groovy.custom;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public final class TraversalProcessStep<S, E> extends MapStep<S, E> implements TraversalParent, Configuring {

    private Traversal.Admin<S, E> mapTraversal;
    private String srcColumn;
    private String dstColumn;

    public TraversalProcessStep(final Traversal.Admin traversal, final Traversal<S, E> mapTraversal) {
        super(traversal);
        this.mapTraversal = this.integrateChild(mapTraversal.asAdmin());
    }

    public Traversal.Admin<S, E> getMapTraversal() {
        return this.mapTraversal;
    }

    @Override
    protected E map(final Traverser.Admin<S> traverser) {
        final Iterator<E> iterator = TraversalUtil.applyAll(traverser, this.mapTraversal);
        return iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    public void configure(final Object... keyValues) {
        if (keyValues.length > 0) {
            srcColumn = (String) keyValues[0];
        }
        if (keyValues.length > 1) {
            dstColumn = (String) keyValues[1];
        }
    }

    @Override
    public Parameters getParameters() {
        return Parameters.EMPTY;
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return Collections.singletonList(this.mapTraversal);
    }

    @Override
    public TraversalProcessStep<S, E> clone() {
        final TraversalProcessStep<S, E> clone = (TraversalProcessStep<S, E>) super.clone();
        clone.mapTraversal = this.mapTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.mapTraversal);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.mapTraversal);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.mapTraversal.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    public String getSrcColumn() {
        return srcColumn;
    }

    public String getDstColumn() {
        return dstColumn;
    }
}

