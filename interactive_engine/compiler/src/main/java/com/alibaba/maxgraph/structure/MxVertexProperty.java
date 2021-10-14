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
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

public class MxVertexProperty<V> implements VertexProperty<V> {

    private final MxVertex vertex;
    private final String key;
    private final V value;

    public MxVertexProperty(MxVertex vertex, String key, V value) {
        this.vertex = vertex;
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() throws NoSuchElementException {
        if (value == null) {
            throw new NoSuchElementException("key: " + key);
        }
        return value;
    }

    @Override
    public boolean isPresent() {
        return this.value != null;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public void remove() {
        TinkerMaxGraph tinkerMaxGraph = (TinkerMaxGraph) vertex.graph();
        GraphVertex graphVertex = (GraphVertex) tinkerMaxGraph.schema().getElement(vertex.label());
        Set<String> propNameList =
                graphVertex.getPrimaryKeyList().stream()
                        .map(GraphProperty::getName)
                        .collect(Collectors.toSet());
        if (propNameList.contains(this.key)) {
            return;
        }

        Iterator<VertexProperty<Object>> propItor = vertex.properties();
        Map<String, Object> kvs = Maps.newHashMap();
        while (propItor.hasNext()) {
            VertexProperty<Object> prop = propItor.next();
            kvs.put(prop.key(), prop.value());
        }
        kvs.remove(this.key);
        tinkerMaxGraph.getBaseGraph().addVertex(vertex.label(), kvs);
    }

    @Override
    public Iterator<Property> properties(String... propertyKeys) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public Object id() {
        return value;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }
}
