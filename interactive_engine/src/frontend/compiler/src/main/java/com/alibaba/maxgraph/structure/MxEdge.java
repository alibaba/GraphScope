/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.structure;

import java.util.Iterator;
import java.util.Map;

import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;

public class MxEdge implements Edge, WrappedEdge<com.alibaba.maxgraph.structure.Edge> {
    private final com.alibaba.maxgraph.structure.Edge baseEdge;
    private TinkerMaxGraph graph;

    public MxEdge(com.alibaba.maxgraph.structure.Edge edge, TinkerMaxGraph graph) {
        this.baseEdge = edge;
        this.graph = graph;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        switch (direction) {
            case OUT:
                return Iterators.singletonIterator(new MxVertex(baseEdge.getSrcVertex(), this.graph));
            case IN:
                return Iterators.singletonIterator(new MxVertex(baseEdge.getDstVertex(), this.graph));
            case BOTH:
                return Lists.newArrayList(
                        (Vertex) new MxVertex(baseEdge.getSrcVertex(), this.graph),
                        (Vertex) new MxVertex(baseEdge.getDstVertex(), this.graph)
                ).iterator();
            default:
                throw new UnsupportedOperationException("not support direction: " + direction);
        }
    }

    @Override
    public Object id() {
        return this.baseEdge.id.id();
    }

    @Override
    public String label() {
        return this.baseEdge.label;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        this.baseEdge.addProperty(key, value);
        return new DefaultProperty<>(key, value, this);
    }

    @Override
    public void remove() {
        this.graph.getBaseGraph()
                .deleteEdge(
                        this.label(),
                        this.baseEdge.id.id(),
                        this.baseEdge.getSrcVertex().id,
                        this.baseEdge.getDstVertex().id);
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        Map<String, Object> kv;
        if (propertyKeys.length == 0) {
            kv = this.baseEdge.getProperties();
        } else {
            kv = this.baseEdge.selectProperties(propertyKeys);
        }

        return kv.entrySet().stream().map(e -> (Property<V>) new DefaultProperty(e.getKey(), e.getValue(), this)).iterator();
    }

    @Override
    public com.alibaba.maxgraph.structure.Edge getBaseEdge() {
        return this.baseEdge;
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
        return StringFactory.edgeString(this);
    }
}
