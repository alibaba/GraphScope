package com.alibaba.graphscope.common.intermediate.process;

import com.google.common.base.Objects;

import java.util.UUID;

public class GraphElement {
    // edge or vertex
    private boolean isEdge;
    // unique id, for hasCode in map
    private UUID uniqueId;

    public GraphElement(boolean isEdge) {
        this.isEdge = isEdge;
        this.uniqueId = UUID.randomUUID();
    }

    public boolean isEdge() {
        return isEdge;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphElement that = (GraphElement) o;
        return isEdge == that.isEdge &&
                Objects.equal(uniqueId, that.uniqueId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(isEdge, uniqueId);
    }
}
