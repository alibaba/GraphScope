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
package com.alibaba.maxgraph.v2.frontend.graph.structure;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphWriteDataException;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.alibaba.maxgraph.v2.frontend.graph.GraphConstants.WRITE_TIMEOUT_MILLSEC;

/**
 * The vertex property in maxgraph
 *
 * @param <V> The property value
 */
public class MaxGraphVertexProperty<V> implements VertexProperty<V> {
    private Vertex vertex;
    private SnapshotMaxGraph graph;
    private String key;
    private V value;
    private boolean present;

    public MaxGraphVertexProperty(Vertex vertex,
                                  SnapshotMaxGraph graph,
                                  String key,
                                  V value,
                                  boolean present) {
        this.vertex = vertex;
        this.graph = graph;
        this.key = key;
        this.value = value;
        this.present = present;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return this.present;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public void remove() {
        try {
            Map<String, Object> removeProperties = Maps.newHashMap();
            removeProperties.put(this.key, null);
            this.graph.getGraphWriter().updateVertexProperties((ElementId) vertex.id(), removeProperties)
                    .get(WRITE_TIMEOUT_MILLSEC, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new GraphWriteDataException("remove edge fail", e);
        }
    }

    @Override
    public Iterator<Property> properties(String... propertyKeys) {
        List<Property> list = Lists.newArrayList();
        return list.iterator();
    }

    @Override
    public Object id() {
        return this.value;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw new GraphWriteDataException("Not support to add property in vertex property");
    }

    @Override
    public boolean equals(Object o) {
        return ElementHelper.areEqual(this, o);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Property) this);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
}
