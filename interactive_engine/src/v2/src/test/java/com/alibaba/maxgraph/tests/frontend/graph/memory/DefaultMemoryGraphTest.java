package com.alibaba.maxgraph.tests.frontend.graph.memory;

import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.alibaba.maxgraph.v2.common.frontend.result.CompositeId;
import com.alibaba.maxgraph.v2.frontend.graph.memory.DefaultMemoryGraph;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultMemoryGraphTest {

    private ElementId buildVertexId1() {
        return new CompositeId(123, 1);
    }

    private Map<String, Object> buildVertex1Properties() {
        Map<String, Object> properties = Maps.newHashMap();
        properties.put("id", 111);
        properties.put("name", "tom");
        return properties;
    }

    private ElementId buildVertexId2() {
        return new CompositeId(124, 2);
    }

    private Map<String, Object> buildVertex2Properties() {
        Map<String, Object> properties = Maps.newHashMap();
        properties.put("id", 222);
        properties.put("lang", "java");
        return properties;
    }

    private ElementId buildVertexId3() {
        return new CompositeId(125, 3);
    }

    private Map<String, Object> buildVertex3Properties() {
        Map<String, Object> properties = Maps.newHashMap();
        properties.put("id", 333);
        properties.put("os", "mac");
        return properties;
    }

    private ElementId buildEdgeId1() {
        return new CompositeId(234, 4);
    }

    private Map<String, Object> buildEdge1Properties() {
        return Maps.newHashMap();
    }

    private ElementId buildEdgeId2() {
        return new CompositeId(235, 5);
    }

    private Map<String, Object> buildEdge2Properties() {
        Map<String, Object> properties = Maps.newHashMap();
        properties.put("weight", 0.4);
        return properties;
    }

    private ElementId buildEdgeId3() {
        return new CompositeId(236, 6);
    }

    private Map<String, Object> buildEdge3Properties() {
        Map<String, Object> properties = Maps.newHashMap();
        properties.put("year", 2018);
        return properties;
    }

    private DefaultMemoryGraph buildSimpleGraph() {
        DefaultMemoryGraph graph = new DefaultMemoryGraph();

        ElementId vertexId1 = buildVertexId1();
        graph.addVertex(vertexId1, buildVertex1Properties());

        ElementId vertexId2 = buildVertexId2();
        graph.addVertex(vertexId2, buildVertex2Properties());

        ElementId vertexId3 = buildVertexId3();
        graph.addVertex(vertexId3, buildVertex3Properties());

        ElementId edgeId1 = buildEdgeId1();
        graph.addEdge(vertexId1, vertexId2, edgeId1, buildEdge1Properties());

        ElementId edgeId2 = buildEdgeId2();
        graph.addEdge(vertexId1, vertexId3, edgeId2, buildEdge2Properties());

        ElementId edgeId3 = buildEdgeId3();
        graph.addEdge(vertexId2, vertexId3, edgeId3, buildEdge3Properties());

        return graph;
    }

    @Test
    void testGetVertex() {
        DefaultMemoryGraph graph = buildSimpleGraph();
        ElementId vertexId1 = new CompositeId(123, 1);
        ElementId vertexId2 = new CompositeId(124, 2);
        ElementId vertexId3 = new CompositeId(125, 3);
        ElementId vertexId4 = new CompositeId(124, 4);

        assertEquals(buildVertex1Properties(), graph.getVertexProperties(vertexId1));
        assertEquals(buildVertex2Properties(), graph.getVertexProperties(vertexId2));
        assertEquals(buildVertex3Properties(), graph.getVertexProperties(vertexId3));
        assertNull(graph.getVertexProperties(vertexId4));

        Map<Integer, Map<Long, Map<String, Object>>> vertices1 = graph.scanVertices(Sets.newHashSet());
        assertEquals(3, vertices1.size());
        assertEquals(Sets.newHashSet(vertexId1.labelId(), vertexId2.labelId(), vertexId3.labelId()),
                vertices1.keySet());
        Map<ElementId, Map<String, Object>> vertexProperties1 = Maps.newHashMap();
        for (Map.Entry<Integer, Map<Long, Map<String, Object>>> entry : vertices1.entrySet()) {
            for (Map.Entry<Long, Map<String, Object>> vertexEntry : entry.getValue().entrySet()) {
                vertexProperties1.put(new CompositeId(vertexEntry.getKey(), entry.getKey()), vertexEntry.getValue());
            }
        }
        assertEquals(3, vertexProperties1.size());
        assertEquals(buildVertex1Properties(), vertexProperties1.get(vertexId1));
        assertEquals(buildVertex2Properties(), vertexProperties1.get(vertexId2));
        assertEquals(buildVertex3Properties(), vertexProperties1.get(vertexId3));

        Map<Integer, Map<Long, Map<String, Object>>> vertices2 = graph.scanVertices(Sets.newHashSet(1, 2));
        assertEquals(2, vertices2.size());
        assertEquals(Sets.newHashSet(vertexId1.labelId(), vertexId2.labelId()),
                vertices2.keySet());
        Map<ElementId, Map<String, Object>> vertexProperties2 = Maps.newHashMap();
        for (Map.Entry<Integer, Map<Long, Map<String, Object>>> entry : vertices2.entrySet()) {
            for (Map.Entry<Long, Map<String, Object>> vertexEntry : entry.getValue().entrySet()) {
                vertexProperties2.put(new CompositeId(vertexEntry.getKey(), entry.getKey()), vertexEntry.getValue());
            }
        }
        assertEquals(2, vertexProperties2.size());
        assertEquals(buildVertex1Properties(), vertexProperties2.get(vertexId1));
        assertEquals(buildVertex2Properties(), vertexProperties2.get(vertexId2));

        Map<Integer, Map<Long, Map<String, Object>>> vertices3 = graph.scanVertices(Sets.newHashSet(111));
        assertTrue(vertices3.isEmpty());
    }

    @Test
    void testGetEdge() {
        DefaultMemoryGraph graph = buildSimpleGraph();

        ElementId edgeId1 = buildEdgeId1();
        DefaultMemoryGraph.DefaultMemoryEdge edge1 = graph.getEdge(edgeId1);
        assertEquals(edge1.getEdgeId(), edgeId1);
        assertEquals(edge1.getProperties(), buildEdge1Properties());
        assertEquals(edge1.getSrcId(), buildVertexId1());
        assertEquals(edge1.getDestId(), buildVertexId2());

        ElementId edgeId2 = buildEdgeId2();
        DefaultMemoryGraph.DefaultMemoryEdge edge2 = graph.getEdge(edgeId2);
        assertEquals(edge2.getEdgeId(), edgeId2);
        assertEquals(edge2.getProperties(), buildEdge2Properties());
        assertEquals(edge2.getSrcId(), buildVertexId1());
        assertEquals(edge2.getDestId(), buildVertexId3());

        List<DefaultMemoryGraph.DefaultMemoryEdge> edgeList1 = graph.scanEdges(Sets.newHashSet());
        assertEquals(3, edgeList1.size());

        List<DefaultMemoryGraph.DefaultMemoryEdge> edgeList2 = graph.scanEdges(Sets.newHashSet(4));
        assertEquals(1, edgeList2.size());

        List<DefaultMemoryGraph.DefaultMemoryEdge> edgeList3 = graph.scanEdges(Sets.newHashSet(111));
        assertTrue(edgeList3.isEmpty());
    }

    @Test
    void testDirectionVertices() {
        DefaultMemoryGraph graph = buildSimpleGraph();

        ElementId vertexId1 = buildVertexId1();
        List<ElementId> vertex1OutIdList = graph.getOutVertices(vertexId1);
        assertEquals(Sets.newHashSet(buildVertexId2(), buildVertexId3()), Sets.newHashSet(vertex1OutIdList));

        List<ElementId> vertex1OutLabel4IdList = graph.getOutVertices(vertexId1, Sets.newHashSet(4));
        assertEquals(1, vertex1OutLabel4IdList.size());
        assertEquals(buildVertexId2(), vertex1OutLabel4IdList.get(0));

        List<ElementId> vertex1OutLabelEmptyIdList = graph.getOutVertices(vertexId1, Sets.newHashSet());
        assertTrue(vertex1OutLabelEmptyIdList.isEmpty());
        List<ElementId> vertex1OutLabelInvalidIdList = graph.getOutVertices(vertexId1, Sets.newHashSet(111));
        assertTrue(vertex1OutLabelInvalidIdList.isEmpty());

        ElementId vertexId3 = buildVertexId3();
        List<ElementId> vertex3InIdList = graph.getInVertices(vertexId3);
        assertEquals(Sets.newHashSet(vertexId1, buildVertexId2()), Sets.newHashSet(vertex3InIdList));

        List<ElementId> vertex3InLabel5IdList = graph.getInVertices(vertexId3, Sets.newHashSet(5));
        assertEquals(1, vertex3InLabel5IdList.size());
        assertEquals(vertexId1, vertex3InLabel5IdList.get(0));

        List<ElementId> vertex3InLabelEmptyIdList = graph.getInVertices(vertexId3, Sets.newHashSet());
        assertTrue(vertex3InLabelEmptyIdList.isEmpty());
        List<ElementId> vertex3InLabelInvalidIdList = graph.getInVertices(vertexId3, Sets.newHashSet(111));
        assertTrue(vertex3InLabelInvalidIdList.isEmpty());
    }

    @Test
    void testDirectionEdges() {
        DefaultMemoryGraph graph = buildSimpleGraph();

        ElementId vertexId1 = buildVertexId1();
        List<DefaultMemoryGraph.DefaultMemoryEdge> vertex1OutEdgeList = graph.getOutEdges(vertexId1);
        for (DefaultMemoryGraph.DefaultMemoryEdge edge : vertex1OutEdgeList) {
            DefaultMemoryGraph.DefaultMemoryEdge graphEdge = graph.getEdge(edge.getEdgeId());
            assertEquals(graphEdge, edge);
        }
        assertEquals(Sets.newHashSet(buildEdgeId1(), buildEdgeId2()),
                vertex1OutEdgeList.stream().map(DefaultMemoryGraph.DefaultMemoryEdge::getEdgeId).collect(Collectors.toSet()));

        List<DefaultMemoryGraph.DefaultMemoryEdge> vertex1OutLabel4EdgeList = graph.getOutEdges(vertexId1, Sets.newHashSet(4));
        assertEquals(1, vertex1OutLabel4EdgeList.size());
        assertEquals(graph.getEdge(buildEdgeId1()), vertex1OutLabel4EdgeList.get(0));

        List<DefaultMemoryGraph.DefaultMemoryEdge> vertex1OutLabelEmptyEdgeList = graph.getOutEdges(vertexId1, Sets.newHashSet());
        assertTrue(vertex1OutLabelEmptyEdgeList.isEmpty());
        List<DefaultMemoryGraph.DefaultMemoryEdge> vertex1OutLabelInvalidEdgeList = graph.getOutEdges(vertexId1, Sets.newHashSet(111));
        assertTrue(vertex1OutLabelInvalidEdgeList.isEmpty());

        ElementId vertexId3 = buildVertexId3();
        List<DefaultMemoryGraph.DefaultMemoryEdge> vertex3InEdgeList = graph.getInEdges(vertexId3);
        for (DefaultMemoryGraph.DefaultMemoryEdge edge : vertex3InEdgeList) {
            DefaultMemoryGraph.DefaultMemoryEdge graphEdge = graph.getEdge(edge.getEdgeId());
            assertEquals(graphEdge, edge);
        }
        assertEquals(Sets.newHashSet(buildEdgeId2(), buildEdgeId3()),
                vertex3InEdgeList.stream().map(DefaultMemoryGraph.DefaultMemoryEdge::getEdgeId).collect(Collectors.toSet()));

        List<DefaultMemoryGraph.DefaultMemoryEdge> vertex3InLabel5EdgeList = graph.getInEdges(vertexId3, Sets.newHashSet(5));
        assertEquals(1, vertex3InLabel5EdgeList.size());
        assertEquals(graph.getEdge(buildEdgeId2()), vertex3InLabel5EdgeList.get(0));

        List<DefaultMemoryGraph.DefaultMemoryEdge> vertex3InLabelEmptyEdgeList = graph.getInEdges(vertexId3, Sets.newHashSet());
        assertTrue(vertex3InLabelEmptyEdgeList.isEmpty());
        List<DefaultMemoryGraph.DefaultMemoryEdge> vertex3InLabelInvalidEdgeList = graph.getInEdges(vertexId3, Sets.newHashSet(111));
        assertTrue(vertex3InLabelInvalidEdgeList.isEmpty());
    }

    @Test
    void testDeleteVertexEdge() {
        DefaultMemoryGraph graph = buildSimpleGraph();

        assertEquals(3, graph.scanEdges(Sets.newHashSet()).size());
        ElementId vertexId1 = buildVertexId1();
        graph.deleteVertex(vertexId1);
        assertEquals(1, graph.scanEdges(Sets.newHashSet()).size());
        assertNull(graph.getVertexProperties(vertexId1));

        ElementId edgeId3 = buildEdgeId3();
        DefaultMemoryGraph.DefaultMemoryEdge edge3 = graph.getEdge(edgeId3);
        graph.deleteEdge(edge3.getSrcId(), edge3.getDestId(), edgeId3);
        assertTrue(graph.scanEdges(Sets.newHashSet()).isEmpty());
        assertEquals(buildVertex2Properties(), graph.getVertexProperties(buildVertexId2()));
        assertEquals(buildVertex3Properties(), graph.getVertexProperties(buildVertexId3()));
    }
}
