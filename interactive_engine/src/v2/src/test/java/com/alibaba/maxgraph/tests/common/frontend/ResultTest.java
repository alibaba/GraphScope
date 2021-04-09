package com.alibaba.maxgraph.tests.common.frontend;

import com.alibaba.maxgraph.v2.common.frontend.result.CompositeId;
import com.alibaba.maxgraph.v2.common.frontend.result.EdgeResult;
import com.alibaba.maxgraph.v2.common.frontend.result.EntryValueResult;
import com.alibaba.maxgraph.v2.common.frontend.result.PathResult;
import com.alibaba.maxgraph.v2.common.frontend.result.PathValueResult;
import com.alibaba.maxgraph.v2.common.frontend.result.PropertyResult;
import com.alibaba.maxgraph.v2.common.frontend.result.ResultConvertUtils;
import com.alibaba.maxgraph.v2.common.frontend.result.VertexPropertyResult;
import com.alibaba.maxgraph.v2.common.frontend.result.VertexResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ResultTest {
    private Map<String, Object> getBaseProperties() {
        Map<String, Object> properties = Maps.newHashMap();
        properties.put("name", "tom");
        properties.put("age", 20);
        return properties;
    }

    private Map<String, Object> getExtendProperties() {
        Map<String, Object> properties = Maps.newHashMap();
        properties.put("weight", 0.55);
        return properties;
    }

    @Test
    void testVertexResult() {
        Map<String, Object> properties = getBaseProperties();
        VertexResult vertexResult = new VertexResult(123, 1, "vertex", getBaseProperties());
        System.out.println(vertexResult.toString());
        assertEquals(123, vertexResult.getId());
        assertEquals(1, vertexResult.getLabelId());
        assertEquals("vertex", vertexResult.getLabel());
        assertEquals(properties, vertexResult.getProperties());

        vertexResult.addProperties(getExtendProperties());
        properties.putAll(getExtendProperties());
        assertEquals(properties, vertexResult.getProperties());

        properties.put("test", "testValue");
        vertexResult.addProperty("test", "testValue");
        assertEquals(properties, vertexResult.getProperties());

        Object object = vertexResult.convertToGremlinStructure();
        assertTrue(object instanceof Vertex);
        Vertex vertex = (Vertex) object;
        Map<String, Object> vertexProperties = Maps.newHashMap();
        Iterator<VertexProperty<Object>> itor = vertex.properties();
        while (itor.hasNext()) {
            VertexProperty<Object> vertexProperty = itor.next();
            assertEquals(vertex, vertexProperty.element());
            vertexProperties.put(vertexProperty.key(), vertexProperty.value());
        }
        assertEquals(properties, vertexProperties);
    }

    @Test
    void testEdgeResult() {
        Map<String, Object> properties = getBaseProperties();
        EdgeResult edgeResult = new EdgeResult(123, 1, "v1", 321, 2, "v2", new CompositeId(111, 3), "edge", getBaseProperties());
        System.out.println(edgeResult.toString());
        assertEquals(new CompositeId(111, 3), edgeResult.getId());
        assertEquals("edge", edgeResult.getLabel());
        assertEquals(123, edgeResult.getSrcVertexId());
        assertEquals(1, edgeResult.getSrcLabelId());
        assertEquals("v1", edgeResult.getSrcLabel());
        assertEquals(321, edgeResult.getDstVertexId());
        assertEquals(2, edgeResult.getDstLabelId());
        assertEquals("v2", edgeResult.getDstLabel());
        assertEquals(properties, edgeResult.getProperties());

        Object object = edgeResult.convertToGremlinStructure();
        assertTrue(object instanceof Edge);
        Edge edge = (Edge) object;
        assertEquals(new CompositeId(123, 1), edge.outVertex().id());
        assertEquals(new CompositeId(321, 2), edge.inVertex().id());

        Map<String, Object> edgeProperties = Maps.newHashMap();
        Iterator<Property<Object>> itor = edge.properties();
        while (itor.hasNext()) {
            Property<Object> property = itor.next();
            edgeProperties.put(property.key(), property.value());
        }
        assertEquals(properties, edgeProperties);

    }

    @Test
    void testPathResult() {
        VertexResult r0 = new VertexResult(123, 1, "vertex1", getBaseProperties());
        VertexResult r1 = new VertexResult(124, 2, "vertex2", getBaseProperties());
        VertexResult r2 = new VertexResult(125, 3, "vertex3", getBaseProperties());
        VertexResult r3 = new VertexResult(126, 4, "vertex4", getBaseProperties());
        List<PathValueResult> pathValueResultList = Lists.newArrayList(
                new PathValueResult(r0),
                new PathValueResult(r1, Sets.newHashSet("a", "b")),
                new PathValueResult(r2),
                new PathValueResult(r3, Sets.newHashSet("c", "d")));
        PathResult pathResult = new PathResult(pathValueResultList);
        System.out.println(pathResult.toString());
        assertEquals(pathValueResultList, pathResult.getPathValueResultList());

        Object object = pathResult.convertToGremlinStructure();
        assertTrue(object instanceof Path);
        Path path = (Path) object;
        assertEquals(pathValueResultList.size(), path.size());

        assertEquals(r0.convertToGremlinStructure(), path.get(0));
        assertEquals(r1.convertToGremlinStructure(), path.get(1));
        assertEquals(r2.convertToGremlinStructure(), path.get(2));
        assertEquals(r3.convertToGremlinStructure(), path.get(3));

        assertTrue(path.labels().get(0).isEmpty());
        assertEquals(Sets.newHashSet("a", "b"), path.labels().get(1));
        assertTrue(path.labels().get(2).isEmpty());
        assertEquals(Sets.newHashSet("c", "d"), path.labels().get(3));
    }

    @Test
    void testPropertyResult() {
        VertexPropertyResult<String> vertexPropertyResult = new VertexPropertyResult<>("name", "tom");
        Object object1 = vertexPropertyResult.convertToGremlinStructure();
        assertTrue(object1 instanceof VertexProperty);
        VertexProperty<String> vertexProperty = (VertexProperty<String>) object1;
        assertEquals("name", vertexProperty.key());
        assertEquals("tom", vertexProperty.value());

        PropertyResult<Integer> propertyResult = new PropertyResult<>("age", 20);
        Object object2 = propertyResult.convertToGremlinStructure();
        assertTrue(object2 instanceof Property);
        Property<Integer> property = (Property<Integer>) object2;
        assertEquals("age", property.key());
        assertEquals(20, property.value());
    }

    @Test
    void testEntryResult() {
        VertexResult vertexResult = new VertexResult(123, 1, "vertex1", getBaseProperties());
        EntryValueResult entryValueResult = new EntryValueResult("v1", vertexResult);
        Object object = entryValueResult.convertToGremlinStructure();
        assertTrue(object instanceof Map.Entry);

        Map.Entry<String, Vertex> entry = (Map.Entry<String, Vertex>) object;
        assertEquals("v1", entry.getKey());
        assertEquals(vertexResult.convertToGremlinStructure(), entry.getValue());
    }

    @Test
    void testResultConvert() {
        VertexResult r0 = new VertexResult(123, 1, "vertex1", getBaseProperties());
        VertexResult r1 = new VertexResult(124, 2, "vertex2", getBaseProperties());
        VertexResult r2 = new VertexResult(125, 3, "vertex3", getBaseProperties());
        VertexResult r3 = new VertexResult(126, 4, "vertex4", getBaseProperties());

        List<VertexResult> vertexList = Lists.newArrayList(r0, r1, r2, r3);
        Object object1 = ResultConvertUtils.convertGremlinResult(vertexList);
        assertTrue(object1 instanceof List);
        List<Vertex> list = (List<Vertex>) object1;
        assertEquals(vertexList.size(), list.size());
        for (int i = 0; i < vertexList.size(); i++) {
            assertEquals(vertexList.get(i).convertToGremlinStructure(), list.get(i));
        }

        Map<VertexResult, VertexResult> vertexMap = Maps.newHashMap();
        vertexMap.put(r0, r1);
        vertexMap.put(r2, r3);
        Object object2 = ResultConvertUtils.convertGremlinResult(vertexMap);
        assertTrue(object2 instanceof Map);
        Map<Vertex, Vertex> map = (Map<Vertex, Vertex>) object2;
        assertEquals(2, map.size());
        assertEquals(r1.convertToGremlinStructure(), map.get(r0.convertToGremlinStructure()));
        assertEquals(r3.convertToGremlinStructure(), map.get(r2.convertToGremlinStructure()));

        assertEquals(123, ResultConvertUtils.convertGremlinResult(123));
        assertEquals("tom", ResultConvertUtils.convertGremlinResult("tom"));
    }
}
