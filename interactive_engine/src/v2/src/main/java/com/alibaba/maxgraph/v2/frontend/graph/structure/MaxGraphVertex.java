package com.alibaba.maxgraph.v2.frontend.graph.structure;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphWriteDataException;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import com.alibaba.maxgraph.v2.frontend.utils.KeyValueUtil;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
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
 * Max graph vertex
 */
public class MaxGraphVertex implements Vertex {
    private SnapshotMaxGraph graph;
    private ElementId id;
    private String label;
    private Map<String, Object> properties;

    public MaxGraphVertex(SnapshotMaxGraph graph,
                          ElementId id,
                          String label) {
        this(graph, id, label, null);
    }

    public MaxGraphVertex(SnapshotMaxGraph graph,
                          ElementId id,
                          String label,
                          Map<String, Object> properties) {
        this.graph = graph;
        this.id = id;
        this.label = label;
        this.properties = properties;
    }

    @Override
    public Edge addEdge(String label, Vertex target, Object... keyValues) {
        // add this->edge->target edge
        Map<String, Object> properties = KeyValueUtil.convertToStringKeyMap(keyValues);
        try {
            ElementId edgeId = this.graph.getGraphWriter()
                    .insertEdge(this.id,
                            (ElementId) target.id(),
                            label,
                            properties)
                    .get(WRITE_TIMEOUT_MILLSEC, TimeUnit.MILLISECONDS);
            return new MaxGraphEdge(this.graph, this, target, edgeId, label, properties);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new GraphWriteDataException("add edge fail", e);
        }
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        if (keyValues.length != 0) {
            throw new GraphWriteDataException("Not support to add properties to vertex property");
        }
        Map<String, Object> addProperties = Maps.newHashMap();
        addProperties.put(key, value);
        try {
            this.graph.getGraphWriter().updateVertexProperties(this.id, addProperties)
                    .get(WRITE_TIMEOUT_MILLSEC, TimeUnit.MILLISECONDS);
            this.properties = null;
            return this.property(key);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new GraphWriteDataException("remove edge fail", e);
        }
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return this.graph.getGraphReader().getEdges(this.id, direction, edgeLabels);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return this.graph.getGraphReader().getVertices(Sets.newHashSet(this.id), direction, edgeLabels);
    }

    @Override
    public Object id() {
        return this.id;
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
        try {
            Iterator<Edge> edgeIterator = this.graph.getGraphReader().getEdges(this.id, Direction.BOTH);
            while (edgeIterator.hasNext()) {
                Edge edge = edgeIterator.next();
                edge.remove();
            }
            this.graph.getGraphWriter().deleteVertex(this.id).get(WRITE_TIMEOUT_MILLSEC, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new GraphWriteDataException("remove edge fail", e);
        }
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        List<VertexProperty<V>> propertyList = Lists.newArrayList();
        if (null == this.properties) {
            Vertex vertex = this.graph.getGraphReader().getVertex(this.id);
            this.properties = Maps.newHashMap();
            Iterator<VertexProperty<Object>> propertyIterator = vertex.properties();
            while (propertyIterator.hasNext()) {
                VertexProperty<Object> vertexProperty = propertyIterator.next();
                this.properties.put(vertexProperty.key(), vertexProperty.value());
            }
        }
        if (propertyKeys.length == 0) {
            for (Map.Entry<String, Object> entry : this.properties.entrySet()) {
                propertyList.add(new MaxGraphVertexProperty(this, this.graph, entry.getKey(), entry.getValue(), true));
            }
        } else {
            for (String propertyKey : propertyKeys) {
                Object propValue = this.properties.get(propertyKey);
                if (null != propValue) {
                    propertyList.add(new MaxGraphVertexProperty(this, this.graph, propertyKey, propValue, true));
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
        return StringFactory.vertexString(this);
    }
}
