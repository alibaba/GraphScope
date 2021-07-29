package org.apache.tinkperpop.gremlin.groovy.custom;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class StringProcessStep<S> extends MapStep<S, Vertex> implements Configuring {

    private String identifier;
    private Map<String, Object> keyValues = new HashMap<>();

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

    @Override
    public void configure(final Object... keyValues) {
        if (keyValues.length > 1) {
            String key = (String) keyValues[0];
            Object value = keyValues[1];
            this.keyValues.put(key, value);
        }
    }

    public Map<String, Object> getKeyValues() {
        return keyValues;
    }
}
