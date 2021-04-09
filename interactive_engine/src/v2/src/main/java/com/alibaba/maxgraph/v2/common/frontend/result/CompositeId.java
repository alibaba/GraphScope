package com.alibaba.maxgraph.v2.common.frontend.result;

import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.google.common.base.Joiner;

/**
 * Implemetion of element id for vertex/edge in maxgraph
 */
public class CompositeId implements ElementId {
    private long id;
    private int labelId;

    public CompositeId() {

    }

    public CompositeId(long id, int labelId) {
        this.id = id;
        this.labelId = labelId;
    }


    @Override
    public long id() {
        return this.id;
    }

    @Override
    public int labelId() {
        return this.labelId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompositeId remoteId = (CompositeId) o;

        if (id != remoteId.id()) {
            return false;
        }
        return labelId == remoteId.labelId();
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + labelId;
        return result;
    }

    @Override
    public String toString() {
        return Joiner.on(".").join(labelId, id);
    }
}