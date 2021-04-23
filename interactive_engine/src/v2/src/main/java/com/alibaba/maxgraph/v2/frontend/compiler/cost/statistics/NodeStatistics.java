package com.alibaba.maxgraph.v2.frontend.compiler.cost.statistics;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaElement;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NodeStatistics {
    private Map<String, Double> vertexCountList;
    private Map<String, Double> edgeCountList;
    private double elementCount;
    private GraphSchema schema;

    public NodeStatistics(GraphSchema schema) {
        this.vertexCountList = Maps.newHashMap();
        this.edgeCountList = Maps.newHashMap();
        this.elementCount = 0;
        this.schema = schema;
    }

    public NodeStatistics(NodeStatistics other) {
        this.vertexCountList = Maps.newHashMap(other.vertexCountList);
        this.edgeCountList = Maps.newHashMap(other.edgeCountList);
        this.elementCount = other.elementCount;
        this.schema = other.schema;
    }

    public Map<String, Double> getVertexCountList() {
        if (vertexCountList.isEmpty() && elementCount > 0) {
            Set<String> vertexLabelList = this.schema.getVertexTypes().stream().map(SchemaElement::getLabel).collect(Collectors.toSet());
            if (!vertexLabelList.isEmpty()) {
                double vertexCount = elementCount / vertexLabelList.size();
                for (String vertexLabel : vertexLabelList) {
                    vertexCountList.put(vertexLabel, vertexCount);
                }
            }
            this.elementCount = 0;
        }
        return vertexCountList;
    }

    public Map<String, Double> getEdgeCountList() {
        if (edgeCountList.isEmpty() && elementCount > 0) {
            Set<String> edgeLabelList = this.schema.getEdgeTypes().stream().map(SchemaElement::getLabel).collect(Collectors.toSet());
            if (!edgeLabelList.isEmpty()) {
                double edgeCount = elementCount / edgeLabelList.size();
                for (String edgeLabel : edgeLabelList) {
                    edgeCountList.put(edgeLabel, edgeCount);
                }
            }
            this.elementCount = 0;
        }
        return edgeCountList;
    }

    public double totalCount() {
        double total = elementCount;
        for (Map.Entry<String, Double> entry : vertexCountList.entrySet()) {
            total += entry.getValue();
        }
        for (Map.Entry<String, Double> entry : edgeCountList.entrySet()) {
            total += entry.getValue();
        }

        return total;
    }

    public void addVertexCount(String vertexLabel, double vertexCount) {
        double currCount = vertexCountList.getOrDefault(vertexLabel, 0.0);
        vertexCountList.put(vertexLabel, currCount + vertexCount);
    }

    public void addEdgeCount(String edgeLabel, double vertexCount) {
        double currCount = edgeCountList.getOrDefault(edgeLabel, 0.0);
        vertexCountList.put(edgeLabel, currCount + vertexCount);
    }

    public void addElementCount(double elementCount) {
        this.elementCount += elementCount;
    }

    public NodeStatistics merge(NodeStatistics other) {
        for (Map.Entry<String, Double> entry : other.vertexCountList.entrySet()) {
            addVertexCount(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, Double> entry : other.edgeCountList.entrySet()) {
            addEdgeCount(entry.getKey(), entry.getValue());
        }
        addElementCount(other.elementCount);

        return this;
    }

    public NodeStatistics merge(NodeStatistics other, double ratio) {
        for (Map.Entry<String, Double> entry : other.vertexCountList.entrySet()) {
            addVertexCount(entry.getKey(), entry.getValue() * ratio);
        }
        for (Map.Entry<String, Double> entry : other.edgeCountList.entrySet()) {
            addEdgeCount(entry.getKey(), entry.getValue() * ratio);
        }
        addElementCount(other.elementCount * ratio);

        return this;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("vertexCountList", vertexCountList)
                .add("edgeCountList", edgeCountList)
                .add("elementCount", elementCount)
                .toString();
    }
}
