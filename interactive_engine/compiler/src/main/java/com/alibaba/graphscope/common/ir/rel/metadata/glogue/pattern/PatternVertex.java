package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import java.util.List;

public abstract class PatternVertex {
    private final ElementDetails details;
    private final IsomorphismChecker isomorphismChecker;

    protected PatternVertex(ElementDetails details, IsomorphismChecker isomorphismChecker) {
        this.details = details;
        this.isomorphismChecker = isomorphismChecker;
    }

    public abstract Integer getId();

    public abstract List<Integer> getVertexTypeIds();

    /// whether this vertex is distinct, i.e., single-typed vertex without any other constraints
    public abstract boolean isDistinct();

    public IsomorphismChecker getIsomorphismChecker() {
        return isomorphismChecker;
    }

    public ElementDetails getDetails() {
        return details;
    }

    @Override
    public String toString() {
        return getId().toString() + "[" + getVertexTypeIds().toString() + "]";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PatternVertex)) {
            return false;
        }
        PatternVertex other = (PatternVertex) o;
        return this.getId().equals(other.getId())
                && this.getVertexTypeIds().equals(other.getVertexTypeIds());
    }
}
