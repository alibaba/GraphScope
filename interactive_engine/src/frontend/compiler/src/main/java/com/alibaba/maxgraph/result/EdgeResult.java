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
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class EdgeResult implements Edge, QueryResult {
    private static final String ID_TAG = "id";
    private VertexResult in;
    private VertexResult out;
    private long id;
    private String label;
    private Map<String, PropertyResult> propertyList;

    public EdgeResult(long id, VertexResult in, VertexResult out, String label) {
        this.id = id;
        this.in = in;
        this.out = out;
        this.label = label;
        this.propertyList = Maps.newHashMap();
    }

    public void addProperty(PropertyResult property) {
        propertyList.put(property.getName(), property);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EdgeResult that = (EdgeResult) o;
        return id == that.id &&
                Objects.equal(in, that.in) &&
                Objects.equal(out, that.out) &&
                Objects.equal(label, that.label) &&
                Objects.equal(propertyList, that.propertyList);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(in, out, id, label, propertyList);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("in", in)
                .add("out", out)
                .add("id", id)
                .add("label", label)
                .add("propertyList", propertyList)
                .toString();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        switch (direction) {
            case OUT:
                return Iterators.singletonIterator(in);
            case IN:
                return Iterators.singletonIterator(out);
            case BOTH:
                List<Vertex> vertexList = Lists.newArrayList();
                vertexList.add(in);
                vertexList.add(out);
                return vertexList.iterator();
            default:
                throw new UnsupportedOperationException("not support direction: " + direction);
        }
    }

    @Override
    public Object id() {
        return id;
    }

    public long getId() {
        return id;
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Graph graph() {
        throw new UnsupportedOperationException("graph");
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw new UnsupportedOperationException("property");
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        List<Property<V>> resultPropertyList = Lists.newArrayList();
        if (propertyKeys.length == 0) {
            for (PropertyResult pr : propertyList.values()) {
                resultPropertyList.add(pr);
            }
        } else {
            for (String propertyKey : propertyKeys) {
                PropertyResult propValue = checkNotNull(propertyList.get(propertyKey));
                resultPropertyList.add(propValue);
            }
        }
        return resultPropertyList.iterator();
    }

    public VertexResult getIn() {
        return out;
    }

    public VertexResult getOut() {
        return in;
    }
}
