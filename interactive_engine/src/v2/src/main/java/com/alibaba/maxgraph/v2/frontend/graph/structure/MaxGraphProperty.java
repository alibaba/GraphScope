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
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.alibaba.maxgraph.v2.frontend.graph.GraphConstants.WRITE_TIMEOUT_MILLSEC;

/**
 * Max graph property
 *
 * @param <V> The property value
 */
public class MaxGraphProperty<V> implements Property<V> {
    private Element element;
    private SnapshotMaxGraph graph;
    private String key;
    private V value;

    public MaxGraphProperty(Element element, SnapshotMaxGraph graph, String key, V value) {
        this.element = element;
        this.graph = graph;
        this.key = key;
        this.value = value;
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
        return true;
    }

    @Override
    public Element element() {
        return this.element;
    }

    @Override
    public void remove() {
        Map<String, Object> removeProperties = Maps.newHashMap();
        removeProperties.put(this.key, null);

        try {
            if (this.element instanceof MaxGraphEdge) {
                MaxGraphEdge edge = (MaxGraphEdge) this.element;
                this.graph.getGraphWriter().updateEdgeProperties((ElementId) edge.outVertex().id(),
                        (ElementId) edge.inVertex().id(),
                        (ElementId) edge.id(),
                        removeProperties)
                        .get(WRITE_TIMEOUT_MILLSEC, TimeUnit.MILLISECONDS);
            } else {
                throw new GraphWriteDataException("invalid element type " + this.element + " for Property.remove()");
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new GraphWriteDataException("remove edge fail", e);
        }
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
        return StringFactory.propertyString(this);
    }
}
