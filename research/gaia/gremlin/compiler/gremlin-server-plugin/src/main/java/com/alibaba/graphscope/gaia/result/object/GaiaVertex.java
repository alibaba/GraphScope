package com.alibaba.graphscope.gaia.result.object;

import com.alibaba.graphscope.common.proto.GremlinResult;
import com.google.common.base.Objects;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class GaiaVertex implements Vertex {
    private long globalId;
    private String label;
    private List<GaiaVertexProperty> propertyList;

    public GaiaVertex(long globalId, String label, List<GaiaVertexProperty> propertyList) {
        this.globalId = globalId;
        this.label = label;
        this.propertyList = propertyList;
    }

    public static GaiaVertex createFromProto(GremlinResult.Vertex vertex) {
        List<GaiaVertexProperty> propertyList = new ArrayList<>();
        vertex.getPropertiesList().forEach(k -> propertyList.add(GaiaVertexProperty.createFromProto(k)));
        // todo: label is id
        return new GaiaVertex(vertex.getId(), vertex.getLabel(), propertyList);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object id() {
        return this.globalId;
    }

    @Override
    public String label() {
        return this.label;
    }

    @Override
    public Graph graph() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
//        return propertyList.stream().filter(entry -> ElementHelper.keyExists(entry.key(), propertyKeys))
//                .map(entry -> (VertexProperty<V>) entry).iterator();
        throw new UnsupportedOperationException("property not support in vertex");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GaiaVertex that = (GaiaVertex) o;
        return globalId == that.globalId &&
                Objects.equal(label, that.label) &&
                Objects.equal(propertyList, that.propertyList);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(globalId, label, propertyList);
    }

    @Override
    public String toString() {
        return "GaiaVertex{" +
                "globalId=" + globalId +
                ", label='" + label + '\'' +
                ", propertyList=" + propertyList +
                '}';
    }
}
