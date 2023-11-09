package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;

import java.util.List;

public abstract class PatternEdge {
    private final boolean isBoth;
    private final ElementDetails details;
    private final IsomorphismChecker isomorphismChecker;

    protected PatternEdge(
            boolean isBoth, ElementDetails details, IsomorphismChecker isomorphismChecker) {
        this.isBoth = isBoth;
        this.details = details;
        this.isomorphismChecker = isomorphismChecker;
    }

    public abstract PatternVertex getSrcVertex();

    public abstract PatternVertex getDstVertex();

    public abstract Integer getId();

    public abstract List<EdgeTypeId> getEdgeTypeIds();

    public abstract boolean isDistinct();

    public boolean isBoth() {
        return this.isBoth;
    }

    public ElementDetails getDetails() {
        return details;
    }

    public IsomorphismChecker getIsomorphismChecker() {
        return isomorphismChecker;
    }

    @Override
    public String toString() {
        return getSrcVertex().getId()
                + "->"
                + getDstVertex().getId()
                + "["
                + getEdgeTypeIds().toString()
                + "]";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PatternEdge)) {
            return false;
        }
        PatternEdge other = (PatternEdge) o;
        return this.getSrcVertex().equals(other.getSrcVertex())
                && this.getDstVertex().equals(other.getDstVertex())
                && this.getEdgeTypeIds().equals(other.getEdgeTypeIds());
    }
}
