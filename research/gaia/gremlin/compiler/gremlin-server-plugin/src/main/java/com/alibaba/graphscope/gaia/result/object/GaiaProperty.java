package com.alibaba.graphscope.gaia.result.object;

import com.alibaba.graphscope.common.proto.GremlinResult;
import com.google.common.base.Objects;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.NoSuchElementException;

public class GaiaProperty<V> implements Property {
    private String key;
    private V value;

    protected GaiaProperty(String key, V value) {
        this.key = key;
        this.value = value;
    }

    public static GaiaProperty createFromProto(GremlinResult.Property property) {
        return new GaiaProperty(property.getKey(), property.getValue());
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public Object value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Vertex element() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GaiaProperty<?> that = (GaiaProperty<?>) o;
        return Objects.equal(key, that.key) &&
                Objects.equal(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key, value);
    }

    @Override
    public String toString() {
        return "GaiaProperty{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
}
