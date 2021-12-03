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

import com.alibaba.maxgraph.sdkcommon.graph.QueryResult;
import com.google.common.base.Objects;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class VertexPropertyResult<V> implements QueryResult, VertexProperty<V> {
    private Map<String, Property> properties;
    private final int id;
    private final String key;
    private final V value;
    private final Vertex vertex;

    public VertexPropertyResult(int id, String key, V value, Vertex vertex) {
        this.id = id;
        this.key = key;
        this.value = value;
        this.vertex = vertex;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Vertex element() {
        return vertex;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    @Override
    public Object id() {
        return value;
    }

    public int getPropId() {
        return id;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        PropertyResult propertyResult = new PropertyResult<>(key, value, this);
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        this.properties.put(key, propertyResult);

        return propertyResult;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        if (null == this.properties) {
            return Collections.emptyIterator();
        } else if (propertyKeys.length == 1) {
            Property<U> property = (Property) this.properties.get(propertyKeys[0]);
            return null == property ? Collections.emptyIterator() : IteratorUtils.of(property);
        } else {
            return ((List) this.properties.entrySet().stream().filter((entry) -> {
                return ElementHelper.keyExists((String) entry.getKey(), propertyKeys);
            }).map((entry) -> {
                return (Property) entry.getValue();
            }).collect(Collectors.toList())).iterator();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VertexPropertyResult<?> that = (VertexPropertyResult<?>) o;
        return id == that.id &&
                Objects.equal(properties, that.properties) &&
                Objects.equal(key, that.key) &&
                Objects.equal(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(properties, id, key, value);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
}
