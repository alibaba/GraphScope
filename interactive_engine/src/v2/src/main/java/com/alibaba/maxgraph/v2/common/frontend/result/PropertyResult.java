package com.alibaba.maxgraph.v2.common.frontend.result;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;

/**
 * Property result in maxgraph
 *
 * @param <V> The property value
 */
public class PropertyResult<V> implements QueryResult {
    private String name;
    private V value;

    public PropertyResult() {

    }

    public PropertyResult(String name, V value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public V getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PropertyResult that = (PropertyResult) o;
        return Objects.equal(name, that.name) &&
                Objects.equal(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, value);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("value", value)
                .toString();
    }

    @Override
    public Object convertToGremlinStructure() {
        return new DetachedProperty<>(this.name, this.value);
    }
}
