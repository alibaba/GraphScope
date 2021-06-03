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

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphQueryDataException;
import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphWriteDataException;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.alibaba.maxgraph.v2.frontend.graph.GraphConstants.WRITE_TIMEOUT_MILLSEC;

/**
 * Max graph edge implementation of gremlin edge
 */
public class MaxGraphEdge implements Edge {
    private SnapshotMaxGraph graph;
    private Vertex src;
    private Vertex dest;
    private ElementId eid;
    private String label;
    private Map<String, Object> properties;

    public MaxGraphEdge(SnapshotMaxGraph graph,
                        Vertex src,
                        Vertex dest,
                        ElementId eid,
                        String label,
                        Map<String, Object> properties) {
        this.graph = graph;
        this.src = src;
        this.dest = dest;
        this.eid = eid;
        this.label = label;
        this.properties = properties;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        List<Vertex> vertexList = Lists.newArrayList();
        switch (direction) {
            case OUT:
                vertexList.add(this.src);
                break;
            case IN:
                vertexList.add(this.dest);
                break;
            case BOTH:
                vertexList.add(this.src);
                vertexList.add(dest);
                break;
            default:
                throw new GraphQueryDataException("Unsupport direction " + direction);
        }
        return vertexList.iterator();
    }

    @Override
    public Object id() {
        return this.eid;
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
    public <V> Property<V> property(String key, V value) {
        Map<String, Object> addProperties = Maps.newHashMap();
        addProperties.put(key, value);
        try {
            this.graph.getGraphWriter().updateEdgeProperties((ElementId) this.src.id(),
                    (ElementId) this.dest.id(),
                    this.eid,
                    addProperties)
                    .get(WRITE_TIMEOUT_MILLSEC, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new GraphWriteDataException("update edge property <" + key + ", " + value + "> fail", e);
        }
        return new MaxGraphProperty<>(this, this.graph, key, value);
    }

    @Override
    public void remove() {
        try {
            this.graph.getGraphWriter().deleteEdge((ElementId) this.src.id(), (ElementId) this.dest.id(), this.eid)
                    .get(WRITE_TIMEOUT_MILLSEC, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new GraphWriteDataException("remove edge fail", e);
        }
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        List<Property<V>> propertyList = Lists.newArrayList();
        if (propertyKeys.length == 0) {
            for (Map.Entry<String, Object> entry : this.properties.entrySet()) {
                propertyList.add(new MaxGraphProperty(this, this.graph, entry.getKey(), entry.getValue()));
            }
        } else {
            for (String propertyKey : propertyKeys) {
                Object propValue = this.properties.get(propertyKey);
                if (null != propValue) {
                    propertyList.add(new MaxGraphProperty(this, this.graph, propertyKey, propValue));
                }
            }
        }
        return propertyList.iterator();
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
