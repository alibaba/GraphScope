/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.structure;

import java.util.Iterator;
import java.util.Map;

import com.alibaba.maxgraph.sdkcommon.graph.CompositeId;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.alibaba.maxgraph.tinkerpop.Utils;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;

import static com.google.common.base.Preconditions.checkNotNull;

public class MxVertex implements Vertex, WrappedVertex<com.alibaba.maxgraph.structure.Vertex> {

    private com.alibaba.maxgraph.structure.Vertex v;
    private TinkerMaxGraph graph;

    public MxVertex(com.alibaba.maxgraph.structure.Vertex v, TinkerMaxGraph graph) {
        checkNotNull(v);
        this.v = v;
        this.graph = graph;
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        MxVertex dst = (MxVertex) inVertex;

        com.alibaba.maxgraph.structure.Edge edge =
                getBaseVertex()
                        .addEdgeTo(dst.getBaseVertex(), label, Utils.convertToMap(keyValues));

        return new MxEdge(edge, this.graph);
    }

    @Override
    public <V> VertexProperty<V> property(
            Cardinality cardinality, String key, V value, Object... keyValues) {
        if (cardinality != Cardinality.single) {
            throw VertexProperty.Exceptions.multiPropertiesNotSupported();
        }

        if (keyValues.length > 0) {
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        }

        this.v.addProperty(key, value);
        return new MxVertexProperty<V>(this, key, value);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {

        Iterator<com.alibaba.maxgraph.structure.Edge> edges;
        switch (direction) {
            case OUT:
                edges = this.v.getOutEdges(edgeLabels);
                break;
            case IN:
                edges = this.v.getInEdges(edgeLabels);
                break;
            case BOTH:
                edges = this.v.getEdges(edgeLabels);
                break;
            default:
                throw new UnsupportedOperationException("not support direction: " + direction);
        }

        return Iterators.transform(edges, e -> new MxEdge(e, this.graph));
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        Iterator<Edge> edges = this.edges(direction, edgeLabels);
        return Iterators.transform(
                edges,
                e -> {
                    switch (direction) {
                        case OUT:
                            return e.inVertex();
                        case IN:
                            return e.outVertex();
                        case BOTH:
                            return e.outVertex().equals(this) ? e.inVertex() : e.outVertex();
                        default:
                            throw new UnsupportedOperationException(
                                    "not support direction: " + direction);
                    }
                });
    }

    @Override
    public Object id() {
        return this.v.id;
    }

    @Override
    public String label() {
        return this.v.label;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public void remove() {
        Iterator<Edge> edgeList = this.edges(Direction.BOTH);
        while (edgeList.hasNext()) {
            Edge edge = edgeList.next();
            edge.remove();
        }
        this.graph.getBaseGraph().deleteVertex((CompositeId) this.v.id);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        Map<String, Object> kv;
        if (propertyKeys.length == 0) {
            kv = this.v.getProperties();
        } else {
            kv = this.v.selectProperties(propertyKeys);
        }
        return kv.entrySet().stream()
                .map(
                        e ->
                                (VertexProperty<V>)
                                        new MxVertexProperty(this, e.getKey(), (V) e.getValue()))
                .iterator();
    }

    @Override
    public com.alibaba.maxgraph.structure.Vertex getBaseVertex() {
        return this.v;
    }

    @Override
    public boolean equals(Object o) {
        return ElementHelper.areEqual(this, o);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
