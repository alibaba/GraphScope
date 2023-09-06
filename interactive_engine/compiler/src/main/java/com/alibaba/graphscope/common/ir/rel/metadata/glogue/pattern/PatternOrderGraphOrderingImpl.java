package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jgrapht.Graph;

// PatternOrdering, preserves the order of vertices in a pattern
// It is used for determining pattern mappings
public class PatternOrderGraphOrderingImpl extends PatternOrder {

    private Map<PatternVertex, Integer> mapVertexToOrder;
    private ArrayList<PatternVertex> mapOrderToVertex;
    private int vertexCount;

    public PatternOrderGraphOrderingImpl(Graph<PatternVertex, PatternEdge> graph) {
        this(graph, new PatternVertexDegreeComparator(graph));
    }

    public PatternOrderGraphOrderingImpl(Graph<PatternVertex, PatternEdge> graph,
            Comparator<PatternVertex> comparator) {

        List<PatternVertex> vertexSet = new ArrayList<>(graph.vertexSet());

        vertexSet.sort(comparator);

        vertexCount = vertexSet.size();
        mapVertexToOrder = new HashMap<>();
        mapOrderToVertex = new ArrayList<>(vertexCount);

        Integer i = 0;
        for (PatternVertex vertex : vertexSet) {
            mapVertexToOrder.put(vertex, i++);
            mapOrderToVertex.add(vertex);
        }
    }

    @Override
    public PatternVertex getVertexByOrder(Integer id) {
        return mapOrderToVertex.get(id);
    }

    @Override
    public Integer getVertexOrder(PatternVertex vertex) {
        return mapVertexToOrder.get(vertex);
    }

    @Override
    public Integer getVertexGroup(PatternVertex vertex) {
        return getVertexOrder(vertex);
    }

    public Map<PatternVertex, Integer> getMapVertexToOrder() {
        return mapVertexToOrder;
    }

    public List<PatternVertex> getMapOrderToVertex() {
        return mapOrderToVertex;
    }

    @Override
    public String toString() {
        return mapVertexToOrder.toString();
    }

    private static class PatternVertexDegreeComparator
            implements
            Comparator<PatternVertex> {
        private Graph<PatternVertex, ?> graph;

        PatternVertexDegreeComparator(Graph<PatternVertex, ?> graph) {
            this.graph = graph;
        }

        @Override
        public int compare(PatternVertex v1, PatternVertex v2) {
            // sort by (degree, outDegree, inDegree, vertexType, position)
            if (graph.degreeOf(v1) - graph.degreeOf(v2) != 0) {
                return graph.degreeOf(v1) - graph.degreeOf(v2);
            } else if (graph.outDegreeOf(v1) - graph.outDegreeOf(v2) != 0) {
                return graph.outDegreeOf(v1) - graph.outDegreeOf(v2);
            } else if (v1.getVertexTypeId() - v2.getVertexTypeId() != 0) {
                return v1.getVertexTypeId() - v2.getVertexTypeId();
            } else {
                return v1.getId() - v2.getId();
            }
        }
    }

}
