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
package com.alibaba.maxgraph.result;

import com.alibaba.maxgraph.sdkcommon.graph.CompositeId;
import com.alibaba.maxgraph.sdkcommon.graph.QueryResult;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.*;

public class VertexResult implements Vertex, QueryResult {
    public final long id;
    public final int labelId;
    public final String label;
    private Set<VertexPropertyResult> propertyList;
    private Graph graph;
    private int storeId;

    public VertexResult(long id, int labelId, String label, Graph graph, int storeId) {
        this.id = id;
        this.labelId = labelId;
        this.label = label;
        this.propertyList = Sets.newHashSet();
        this.graph = graph;
        this.storeId = storeId;
    }

    public void addProperty(VertexPropertyResult propertyResult) {
        this.propertyList.add(propertyResult);
    }

    public Set<VertexPropertyResult> getPropertyList() {
        return propertyList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VertexResult that = (VertexResult) o;
        return id == that.id &&
                Objects.equal(label, that.label) &&
                Objects.equal(propertyList, that.propertyList);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, label, propertyList);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", id)
                .add("label", label)
                .add("propertyList", propertyList).toString();
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

    public Iterator<Vertex> self(){
        List<Vertex> vertices = new ArrayList<>();
        vertices.add(this);
        return vertices.iterator();
    }

    @Override
    public Object id() {
        return new CompositeId(id, labelId);
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        return null == this.propertyList ?
                Collections.emptyIterator():
                (Iterator)propertyList.stream().filter(entry -> ElementHelper.keyExists(entry.key(), propertyKeys)).map(entry -> (VertexProperty<V>)entry).iterator();
    }

    public int getStoreId() {
        return this.storeId;
    }
}
