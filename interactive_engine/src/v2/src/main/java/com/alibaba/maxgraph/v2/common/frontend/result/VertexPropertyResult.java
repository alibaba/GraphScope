package com.alibaba.maxgraph.v2.common.frontend.result;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;

/**
 * Vertex property result
 *
 * @param <V> The property value
 */
public class VertexPropertyResult<V> extends PropertyResult<V> {
    public VertexPropertyResult() {
        super();
    }

    public VertexPropertyResult(String name, V value) {
        super(name, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VertexPropertyResult that = (VertexPropertyResult) o;
        return Objects.equal(this.getName(), that.getName()) &&
                Objects.equal(this.getValue(), that.getValue());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .toString();
    }

    @Override
    public Object convertToGremlinStructure() {
        return DetachedVertexProperty.build().setId(this.getValue())
                .setLabel(this.getName())
                .setValue(this.getValue())
                .create();
    }
}
