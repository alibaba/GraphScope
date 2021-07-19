package org.apache.tinkperpop.gremlin.groovy.custom;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Objects;

public final class StringProcessStep<S> extends MapStep<S, Vertex> implements Mutating<Event> {

    private String identifier;

    public StringProcessStep(final Traversal.Admin traversal, final String identifier) {
        super(traversal);
        this.identifier = identifier;
    }

    public String getIdentifier() {
        return identifier;
    }

    @Override
    protected Vertex map(final Traverser.Admin<S> traverser) {
        return null;
    }

    @Override
    public CallbackRegistry<Event> getMutatingCallbackRegistry() {
        return null;
    }
    @Override
    public void configure(final Object... keyValues) {
        // do nothing
    }
    @Override
    public Parameters getParameters() {
        return Parameters.EMPTY;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.identifier);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.identifier);
    }

}
