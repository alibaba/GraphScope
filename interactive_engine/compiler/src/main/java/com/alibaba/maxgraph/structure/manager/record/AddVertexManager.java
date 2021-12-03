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
package com.alibaba.maxgraph.structure.manager.record;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;
import java.util.Map;

public class AddVertexManager implements Vertex, RecordManager {
    private String label;
    private Map<String, Object> propertyList;
    private Graph graph;

    public AddVertexManager(String label, Map<String, Object> propertyList, Graph graph) {
        this.label = label;
        this.propertyList = propertyList;
        this.graph = graph;
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
        throw new UnsupportedOperationException();
    }

    @Override
    public String label() {
        return this.label;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Object> getPropertyList() {
        return this.propertyList;
    }
}
