package com.alibaba.graphscope.gaia.result.object;

import com.alibaba.graphscope.common.proto.GremlinResult;
import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class GaiaEdge implements Edge {
    private long id;
    private String label;
    private GaiaVertex in;
    private GaiaVertex out;
    private List<GaiaProperty> propertyList;

    private GaiaEdge(long id, String label, GaiaVertex in, GaiaVertex out, List<GaiaProperty> propertyList) {
        this.id = id;
        this.label = label;
        this.in = in;
        this.out = out;
        this.propertyList = propertyList;
    }

    public static GaiaEdge createFromProto(GremlinResult.Edge edge) {
        // label is id
        return new GaiaEdge(edge.getId(), edge.getLabel(),
                new GaiaVertex(edge.getDstId(), edge.getDstLabel(), Collections.EMPTY_LIST),
                new GaiaVertex(edge.getSrcId(), edge.getSrcLabel(), Collections.EMPTY_LIST), Collections.EMPTY_LIST);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        switch (direction) {
            case BOTH:
                List<Vertex> both = new ArrayList<>();
                both.add(in);
                both.add(out);
                return both.iterator();
            case IN:
                return Iterators.singletonIterator(in);
            case OUT:
                return Iterators.singletonIterator(out);
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public Object id() {
        return this.id;
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
    public <V> Property<V> property(String key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
//        return propertyList.stream().filter(entry -> ElementHelper.keyExists(entry.key(), propertyKeys))
//                .map(entry -> (Property<V>) entry).iterator();
        throw new UnsupportedOperationException("property not support in edge");
    }

    @Override
    public String toString() {
        return "GaiaEdge{" +
                "id=" + id +
                ", label='" + label + '\'' +
                ", in=" + in +
                ", out=" + out +
                ", propertyList=" + propertyList +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GaiaEdge gaiaEdge = (GaiaEdge) o;
        return id == gaiaEdge.id &&
                Objects.equal(label, gaiaEdge.label) &&
                Objects.equal(in, gaiaEdge.in) &&
                Objects.equal(out, gaiaEdge.out) &&
                Objects.equal(propertyList, gaiaEdge.propertyList);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, label, in, out, propertyList);
    }
}
