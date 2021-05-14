package com.alibaba.graphscope.gaia.result.object;

import com.alibaba.graphscope.common.proto.GremlinResult;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;

public final class GaiaVertexProperty<V> extends GaiaProperty implements VertexProperty {
    private GaiaVertexProperty(String key, V value) {
        super(key, value);
    }

    public static GaiaVertexProperty createFromProto(GremlinResult.Property property) {
        return new GaiaVertexProperty(property.getKey(), property.getValue());
    }

    @Override
    public Iterator<Property> properties(String... propertyKeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object id() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw new UnsupportedOperationException();
    }
}
