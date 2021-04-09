package com.alibaba.maxgraph.tests.frontend.graph;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphWriteDataException;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphWriter;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.graph.structure.MaxGraphEdge;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GraphWriterReaderTestHelper {
    static void testCreateGraphData(Graph graph, MaxGraphWriter writer, GraphSchema schema) throws Exception {
        GraphSchemaTestHelper.createModernSchema(schema, writer);

        Map<Long, Element> idElementMap = genModernGraph(graph);
        validateModernGraph(graph, idElementMap);
    }

    static void testVertexEdgeProperty(Graph graph, MaxGraphWriter writer, GraphSchema schema) throws Exception {
        GraphSchemaTestHelper.createModernSchema(schema, writer);

        Map<Long, Element> idElementMap = genModernGraph(graph);

        // add batch vertex and edges
        Map<String, Object> v100Properties = Maps.newHashMap();
        v100Properties.put("id", 100L);
        Map<String, Object> v101Properties = Maps.newHashMap();
        v100Properties.put("id", 101L);
        List<ElementId> vertexIds = writer.insertVertices(Lists.newArrayList(
                Pair.of("person", v100Properties),
                Pair.of("person", v101Properties)
        )).get();
        assertEquals(2, vertexIds.size());
        List<ElementId> edgeIds = writer.insertEdges(Lists.newArrayList(
                Triple.of(Pair.of(vertexIds.get(0), vertexIds.get(1)),
                        "knows",
                        Maps.newHashMap()),
                Triple.of(Pair.of(vertexIds.get(1), vertexIds.get(0)),
                        "knows",
                        Maps.newHashMap()))).get();
        assertEquals(2, edgeIds.size());

        // remove batch vertex/edges
        List<Triple<ElementId, ElementId, ElementId>> removeBatchEdgeIds = Lists.newArrayList();
        graph.edges(edgeIds.toArray()).forEachRemaining(e -> removeBatchEdgeIds.add(Triple.of((ElementId) e.outVertex().id(), (ElementId) e.inVertex().id(), (ElementId) e.id())));
        assertEquals(2, removeBatchEdgeIds.size());
        writer.deleteEdges(removeBatchEdgeIds).get();
        assertFalse(graph.edges(edgeIds.toArray()).hasNext());
        writer.deleteVertices(Sets.newHashSet(vertexIds)).get();
        assertFalse(graph.vertices(vertexIds.toArray()).hasNext());

        // remove vertex property
        ElementId v1Id = (ElementId) idElementMap.get(1L).id();
        Vertex v1 = graph.vertices(v1Id).next();
        v1.property("name").remove();
        assertEquals("person", v1.label());
        assertNotNull(v1.toString());
        assertThrows(GraphWriteDataException.class,
                () -> v1.property("name", "testNameValue", "aa", "bb"));
        assertFalse(graph.vertices(v1Id).next().property("name").isPresent());
        VertexProperty newV1NameProperty = v1.property("name", "new name");
        assertThrows(GraphWriteDataException.class, () -> newV1NameProperty.property("testKey", "testValue"));
        assertFalse(newV1NameProperty.properties().hasNext());
        assertEquals("new name", newV1NameProperty.id());
        assertNotNull(newV1NameProperty.toString());
        assertEquals(1L, newV1NameProperty.element().property("id").value());
        Vertex newV1 = graph.vertices(v1Id).next();
        assertTrue(newV1.property("name").isPresent());
        assertEquals("new name", newV1.property("name").value());

        // remove edge property
        ElementId e7Id = (ElementId) idElementMap.get(7L).id();
        Edge e7 = graph.edges(e7Id).next();
        e7.property("weight").remove();
        assertFalse(graph.edges(e7Id).next().property("weight").isPresent());
        e7.property("weight", 1.0);
        Edge newE7 = graph.edges(e7Id).next();
        assertTrue(newE7.property("weight").isPresent());
        assertEquals(1.0, newE7.property("weight").value());

        // remove vertex
        v1.remove();
        assertFalse(graph.vertices(v1Id).hasNext());
        assertFalse(graph.edges(e7Id,
                idElementMap.get(8L).id(),
                idElementMap.get(9L).id()).hasNext());
        int v1RemoveVertexCount = 0;
        int v1RemoveEdgeCount = 0;
        Iterator<Vertex> v1RemoveVertices = graph.vertices();
        while (v1RemoveVertices.hasNext()) {
            v1RemoveVertices.next();
            v1RemoveVertexCount++;
        }
        Iterator<Edge> v1RemoveEdges = graph.edges();
        while (v1RemoveEdges.hasNext()) {
            v1RemoveEdges.next();
            v1RemoveEdgeCount++;
        }
        assertEquals(5, v1RemoveVertexCount);
        assertEquals(3, v1RemoveEdgeCount);

        // remove edge
        ElementId e11Id = (ElementId) idElementMap.get(11L).id();
        Edge edge11 = graph.edges(e11Id).next();
        Property edge11IdProperty = edge11.property("id");
        assertEquals("id", edge11IdProperty.key());
        assertTrue(StringUtils.startsWith(edge11IdProperty.toString(), "p["));
        Set<Long> edge11BothVertexIds = Sets.newHashSet();
        edge11.vertices(Direction.BOTH).forEachRemaining(v -> edge11BothVertexIds.add((Long) v.property("id").value()));
        assertEquals(Sets.newHashSet(3L, 4L), edge11BothVertexIds);
        assertEquals("created", edge11.label());
        assertNotNull(edge11.graph());
        assertNotNull(edge11.toString());
        Element e11Element = edge11IdProperty.element();
        e11Element.remove();
        int e11RemoveEdgeCount = 0;
        Iterator<Edge> e11RemoveEdges = graph.edges();
        while (e11RemoveEdges.hasNext()) {
            e11RemoveEdges.next();
            e11RemoveEdgeCount++;
        }
        assertEquals(2, e11RemoveEdgeCount);
        assertFalse(graph.edges(e11Id).hasNext());

    }

    static void testDirectionElementProperty(Graph graph, MaxGraphWriter writer, GraphSchema schema) throws Exception {
        GraphSchemaTestHelper.createModernSchema(schema, writer);

        Map<Long, Element> idElementMap = genModernGraph(graph);
        ElementId v1Id = (ElementId) idElementMap.get(1L).id();
        Vertex v1 = graph.vertices(v1Id).next();

        List<Element> v1OutVertexList = Lists.newArrayList();
        v1.vertices(Direction.OUT).forEachRemaining(v1OutVertexList::add);
        checkDirectionElementList(idElementMap, v1OutVertexList, 3, Sets.newHashSet(2L, 3L, 4L));

        List<Element> v1OutEdgeList = Lists.newArrayList();
        v1.edges(Direction.OUT).forEachRemaining(v1OutEdgeList::add);
        checkDirectionElementList(idElementMap, v1OutEdgeList, 3, Sets.newHashSet(7L, 8L, 9L));

        List<Element> v1OutEdgeVertexList = Lists.newArrayList();
        v1OutEdgeList.forEach(e -> v1OutEdgeVertexList.add(((MaxGraphEdge) e).inVertex()));
        checkDirectionElementList(idElementMap, v1OutEdgeVertexList, 3, Sets.newHashSet(2L, 3L, 4L));

        List<Element> v1OutKnowsVertexList = Lists.newArrayList();
        v1.vertices(Direction.OUT, "knows").forEachRemaining(v1OutKnowsVertexList::add);
        checkDirectionElementList(idElementMap, v1OutKnowsVertexList, 2, Sets.newHashSet(2L, 4L));

        List<Element> v1OutKnowsEdgeList = Lists.newArrayList();
        v1.edges(Direction.OUT, "knows").forEachRemaining(v1OutKnowsEdgeList::add);
        checkDirectionElementList(idElementMap, v1OutKnowsEdgeList, 2, Sets.newHashSet(7L, 8L));

        List<Element> v1OutKnowsEdgeVertexList = Lists.newArrayList();
        v1OutKnowsEdgeList.forEach(e -> v1OutKnowsEdgeVertexList.add(((MaxGraphEdge) e).inVertex()));
        checkDirectionElementList(idElementMap, v1OutKnowsEdgeVertexList, 2, Sets.newHashSet(2L, 4L));

        ElementId v3Id = (ElementId) idElementMap.get(3L).id();
        Vertex v3 = graph.vertices(v3Id).next();

        List<Element> v3InVertexList = Lists.newArrayList();
        v3.vertices(Direction.IN).forEachRemaining(v3InVertexList::add);
        checkDirectionElementList(idElementMap, v3InVertexList, 3, Sets.newHashSet(1L, 4L, 6L));

        List<Element> v3InEdgeList = Lists.newArrayList();
        v3.edges(Direction.IN).forEachRemaining(v3InEdgeList::add);
        checkDirectionElementList(idElementMap, v3InEdgeList, 3, Sets.newHashSet(9L, 11L, 12L));

        List<Element> v3InEdgeVertexList = Lists.newArrayList();
        v3InEdgeList.forEach(e -> v3InEdgeVertexList.add(((MaxGraphEdge) e).outVertex()));
        checkDirectionElementList(idElementMap, v3InEdgeVertexList, 3, Sets.newHashSet(1L, 4L, 6L));

        List<Element> v3InCreatedVertexList = Lists.newArrayList();
        v3.vertices(Direction.IN, "created").forEachRemaining(v3InCreatedVertexList::add);
        checkDirectionElementList(idElementMap, v3InCreatedVertexList, 3, Sets.newHashSet(1L, 4L, 6L));

        List<Element> v3InCreatedEdgeList = Lists.newArrayList();
        v3.edges(Direction.IN, "created").forEachRemaining(v3InCreatedEdgeList::add);
        checkDirectionElementList(idElementMap, v3InCreatedEdgeList, 3, Sets.newHashSet(9L, 11L, 12L));

        List<Element> v3InCreatedEdgeVertexList = Lists.newArrayList();
        v3InCreatedEdgeList.forEach(e -> v3InCreatedEdgeVertexList.add(((MaxGraphEdge) e).outVertex()));
        checkDirectionElementList(idElementMap, v3InCreatedEdgeVertexList, 3, Sets.newHashSet(1L, 4L, 6L));
    }

    static void testMaxTinkerGraph(Graph graph) throws Exception {
        assertThrows(UnsupportedOperationException.class, () -> graph.compute(null));
        assertThrows(UnsupportedOperationException.class, graph::compute);
        assertThrows(UnsupportedOperationException.class, graph::tx);

        Graph.Features features = graph.features();

        Graph.Features.GraphFeatures graphFeatures = features.graph();
        assertFalse(graphFeatures.supportsTransactions());
        assertFalse(graphFeatures.supportsThreadedTransactions());
        assertFalse(graphFeatures.supportsComputer());
        assertFalse(graphFeatures.supportsIoRead());
        assertFalse(graphFeatures.supportsIoWrite());
        assertTrue(graphFeatures.supportsPersistence());
        assertTrue(graphFeatures.supportsConcurrentAccess());

        Graph.Features.VariableFeatures variableFeatures = graphFeatures.variables();
        assertTrue(variableFeatures.supportsBooleanValues());
        assertTrue(variableFeatures.supportsDoubleValues());
        assertTrue(variableFeatures.supportsFloatValues());
        assertTrue(variableFeatures.supportsIntegerValues());
        assertTrue(variableFeatures.supportsLongValues());
        assertTrue(variableFeatures.supportsBooleanArrayValues());
        assertTrue(variableFeatures.supportsStringValues());
        assertFalse(variableFeatures.supportsDoubleArrayValues());
        assertFalse(variableFeatures.supportsFloatArrayValues());
        assertFalse(variableFeatures.supportsIntegerArrayValues());
        assertFalse(variableFeatures.supportsLongArrayValues());
        assertFalse(variableFeatures.supportsStringArrayValues());
        assertFalse(variableFeatures.supportsMapValues());
        assertFalse(variableFeatures.supportsMixedListValues());
        assertFalse(variableFeatures.supportsByteValues());
        assertFalse(variableFeatures.supportsByteArrayValues());
        assertFalse(variableFeatures.supportsSerializableValues());
        assertFalse(variableFeatures.supportsUniformListValues());

        Graph.Features.VertexFeatures vertexFeatures = features.vertex();
        assertEquals(VertexProperty.Cardinality.single, vertexFeatures.getCardinality(null));
        assertTrue(vertexFeatures.supportsAddVertices());
        assertTrue(vertexFeatures.supportsRemoveVertices());
        assertFalse(vertexFeatures.supportsMultiProperties());
        assertFalse(vertexFeatures.supportsMetaProperties());
        Graph.Features.VertexPropertyFeatures vertexPropertyFeatures = vertexFeatures.properties();
        assertTrue(vertexPropertyFeatures.supportsRemoveProperty());
        assertFalse(vertexPropertyFeatures.supportsUserSuppliedIds());
        assertFalse(vertexPropertyFeatures.supportsNumericIds());
        assertFalse(vertexPropertyFeatures.supportsStringIds());
        assertFalse(vertexPropertyFeatures.supportsUuidIds());
        assertFalse(vertexPropertyFeatures.supportsCustomIds());
        assertFalse(vertexPropertyFeatures.supportsAnyIds());
        assertFalse(vertexPropertyFeatures.supportsMapValues());
        assertFalse(vertexPropertyFeatures.supportsMixedListValues());
        assertFalse(vertexPropertyFeatures.supportsSerializableValues());
        assertFalse(vertexPropertyFeatures.supportsUniformListValues());

        Graph.Features.EdgeFeatures edgeFeatures = features.edge();
        assertTrue(edgeFeatures.supportsAddEdges());
        assertTrue(edgeFeatures.supportsRemoveEdges());
        Graph.Features.EdgePropertyFeatures edgePropertyFeatures = edgeFeatures.properties();
        assertTrue(edgePropertyFeatures.supportsBooleanValues());
        assertTrue(edgePropertyFeatures.supportsByteValues());
        assertTrue(edgePropertyFeatures.supportsDoubleValues());
        assertTrue(edgePropertyFeatures.supportsFloatValues());
        assertTrue(edgePropertyFeatures.supportsIntegerValues());
        assertTrue(edgePropertyFeatures.supportsLongValues());
        assertTrue(edgePropertyFeatures.supportsByteArrayValues());
        assertTrue(edgePropertyFeatures.supportsDoubleArrayValues());
        assertTrue(edgePropertyFeatures.supportsFloatArrayValues());
        assertTrue(edgePropertyFeatures.supportsIntegerArrayValues());
        assertTrue(edgePropertyFeatures.supportsStringArrayValues());
        assertTrue(edgePropertyFeatures.supportsLongArrayValues());
        assertTrue(edgePropertyFeatures.supportsStringValues());
        assertFalse(edgePropertyFeatures.supportsMapValues());
        assertFalse(edgePropertyFeatures.supportsMixedListValues());
        assertFalse(edgePropertyFeatures.supportsBooleanArrayValues());
        assertFalse(edgePropertyFeatures.supportsSerializableValues());
        assertFalse(edgePropertyFeatures.supportsUniformListValues());

        assertNotNull(features.toString());

        Graph.Variables variables = graph.variables();
        variables.set("test", "testValue");
        assertTrue(variables.get("test").isPresent());
        variables.remove("test");
        assertFalse(variables.get("test").isPresent());
        assertTrue(variables.keys().isEmpty());
        assertNotNull(variables.toString());

        assertNotNull(graph.configuration());
        graph.close();
    }

    private static Map<Long, Element> genModernGraph(Graph graph) {
        Map<Long, Element> idElementMap = Maps.newHashMap();

        // add person vertices
        Vertex v1 = graph.addVertex(T.label, "person", "id", 1L, "name", "marko", "age", 29);
        Vertex v2 = graph.addVertex(T.label, "person", "id", 2L, "name", "marko", "age", 27);
        Vertex v4 = graph.addVertex(T.label, "person", "id", 4L, "name", "marko", "age", 23);
        Vertex v6 = graph.addVertex(T.label, "person", "id", 6L, "name", "marko", "age", 35);

        // add software vertices
        Vertex v3 = graph.addVertex(T.label, "software", "id", 3L, "name", "lop", "lang", "java");
        Vertex v5 = graph.addVertex(T.label, "software", "id", 5L, "name", "ripple", "lang", "java");

        // add knows edges
        Edge e7 = v1.addEdge("knows", v2, "id", 7L, "weight", 0.5);
        Edge e8 = v1.addEdge("knows", v4, "id", 8L, "weight", 1.0);

        // add created edges
        Edge e9 = v1.addEdge("created", v3, "id", 9L, "weight", 0.4);
        Edge e10 = v4.addEdge("created", v5, "id", 10L, "weight", 1.0);
        Edge e11 = v4.addEdge("created", v3, "id", 11L, "weight", 0.4);
        Edge e12 = v6.addEdge("created", v3, "id", 12L, "weight", 0.2);

        idElementMap.put(1L, v1);
        idElementMap.put(2L, v2);
        idElementMap.put(3L, v3);
        idElementMap.put(4L, v4);
        idElementMap.put(5L, v5);
        idElementMap.put(6L, v6);
        idElementMap.put(7L, e7);
        idElementMap.put(8L, e8);
        idElementMap.put(9L, e9);
        idElementMap.put(10L, e10);
        idElementMap.put(11L, e11);
        idElementMap.put(12L, e12);
        return idElementMap;
    }

    private static void validateModernGraph(Graph graph, Map<Long, Element> idElementMap) {
        Iterator<Vertex> vertexIterator = graph.vertices();
        int vertexCount = 0;
        while (vertexIterator.hasNext()) {
            Vertex vertex = vertexIterator.next();
            long id = (Long) vertex.property("id").value();
            Element element = idElementMap.get(id);
            checkElementProperties(element, vertex);
            vertexCount++;
        }
        assertEquals(6, vertexCount);

        Iterator<Edge> edgeIterator = graph.edges();
        int edgeCount = 0;
        while (edgeIterator.hasNext()) {
            Edge edge = edgeIterator.next();
            long id = (Long) edge.property("id").value();
            Element element = idElementMap.get(id);
            checkElementProperties(element, edge);
            edgeCount++;
        }
        assertEquals(6, edgeCount);
    }

    private static void checkElementProperties(Element expectedElement, Element resultElement) {
        assertEquals(expectedElement.getClass(), resultElement.getClass());
        Iterator<Property<Object>> expectedPropertyIterator = (Iterator<Property<Object>>) expectedElement.properties();
        Map<String, Object> expectedProperties = Maps.newHashMap();
        while (expectedPropertyIterator.hasNext()) {
            Property property = expectedPropertyIterator.next();
            expectedProperties.put(property.key(), property.value());
        }

        Iterator<Property<Object>> resultPropertyIterator = (Iterator<Property<Object>>) resultElement.properties();
        Map<String, Object> resultProperties = Maps.newHashMap();
        while (resultPropertyIterator.hasNext()) {
            Property property = resultPropertyIterator.next();
            resultProperties.put(property.key(), property.value());
        }

        assertEquals(expectedProperties, resultProperties);
    }

    private static void checkDirectionElementList(Map<Long, Element> idElementMap,
                                                  List<Element> resultElementList,
                                                  int expectedCount,
                                                  Set<Long> idSet) {
        assertEquals(expectedCount, resultElementList.size());
        assertEquals(idSet,
                resultElementList.stream()
                        .map(v -> v.property("id").value())
                        .collect(Collectors.toSet()));
        resultElementList.forEach(v -> {
            checkElementProperties(v, idElementMap.get(v.property("id").value()));
        });
    }
}
