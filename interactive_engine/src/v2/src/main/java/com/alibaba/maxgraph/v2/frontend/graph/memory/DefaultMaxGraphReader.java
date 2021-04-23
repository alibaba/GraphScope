package com.alibaba.maxgraph.v2.frontend.graph.memory;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphQueryDataException;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphReader;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphWriter;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SnapshotSchema;
import com.alibaba.maxgraph.v2.common.frontend.result.CompositeId;
import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import com.alibaba.maxgraph.v2.frontend.graph.structure.MaxGraphEdge;
import com.alibaba.maxgraph.v2.frontend.graph.structure.MaxGraphVertex;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Memory max graph reader for test
 */
public class DefaultMaxGraphReader implements MaxGraphReader {
    private DefaultMemoryGraph graph;
    private SnapshotSchema snapshotSchema;
    private SnapshotMaxGraph maxTinkerGraph;

    public DefaultMaxGraphReader(MaxGraphWriter writer,
                                 DefaultMemoryGraph graph,
                                 SchemaFetcher schemaFetcher) {
        this.graph = graph;
        this.snapshotSchema = schemaFetcher.fetchSchema();
        this.maxTinkerGraph = new SnapshotMaxGraph();
        this.maxTinkerGraph.initialize(this, writer, schemaFetcher);
    }

    @Override
    public Vertex getVertex(ElementId vertexId) {
        Map<String, Object> properties = graph.getVertexProperties(vertexId);
        return new MaxGraphVertex(maxTinkerGraph, vertexId, snapshotSchema.getSchema().getSchemaElement(vertexId.labelId()).getLabel(), properties);
    }

    @Override
    public Iterator<Vertex> getVertices(Set<ElementId> vertexIds) {
        List<Vertex> vertexList = Lists.newArrayListWithCapacity(vertexIds.size());
        for (ElementId vertexId : vertexIds) {
            Map<String, Object> properties = graph.getVertexProperties(vertexId);
            if (null != properties) {
                MaxGraphVertex vertex = new MaxGraphVertex(maxTinkerGraph, vertexId, snapshotSchema.getSchema().getSchemaElement(vertexId.labelId()).getLabel(), properties);
                vertexList.add(vertex);
            }
        }
        return vertexList.iterator();
    }

    @Override
    public Iterator<Vertex> getVertices(Set<ElementId> vertexIds, Direction direction, String... edgeLabels) {
        List<Vertex> resultList = Lists.newArrayList();
        for (ElementId vertexId : vertexIds) {
            if (edgeLabels.length == 0) {
                switch (direction) {
                    case OUT:
                        processVertexIds(resultList, snapshotSchema.getSchema(), this.graph.getOutVertices(vertexId));
                        break;
                    case IN:
                        processVertexIds(resultList, snapshotSchema.getSchema(), this.graph.getInVertices(vertexId));
                        break;
                    case BOTH:
                        processVertexIds(resultList, snapshotSchema.getSchema(), this.graph.getOutVertices(vertexId));
                        processVertexIds(resultList, snapshotSchema.getSchema(), this.graph.getInVertices(vertexId));
                        break;
                    default:
                        throw new GraphQueryDataException("Unsupport direction " + direction);
                }
            } else {
                Set<Integer> labelIdSet = Sets.newHashSet();
                for (String edgeLabel : edgeLabels) {
                    labelIdSet.add(snapshotSchema.getSchema().getSchemaElement(edgeLabel).getLabelId());
                }
                switch (direction) {
                    case OUT:
                        processVertexIds(resultList, snapshotSchema.getSchema(), this.graph.getOutVertices(vertexId, labelIdSet));
                        break;
                    case IN:
                        processVertexIds(resultList, snapshotSchema.getSchema(), this.graph.getInVertices(vertexId, labelIdSet));
                        break;
                    case BOTH:
                        processVertexIds(resultList, snapshotSchema.getSchema(), this.graph.getOutVertices(vertexId, labelIdSet));
                        processVertexIds(resultList, snapshotSchema.getSchema(), this.graph.getInVertices(vertexId, labelIdSet));
                        break;
                    default:
                        throw new GraphQueryDataException("Unsupport direction " + direction);
                }
            }
        }
        return resultList.iterator();
    }

    private void processVertexIds(List<Vertex> resultList, GraphSchema schema, List<ElementId> outVertices) {
        resultList.addAll(outVertices
                .stream()
                .map(v -> new MaxGraphVertex(this.maxTinkerGraph,
                        v,
                        schema.getSchemaElement(v.labelId()).getLabel()))
                .collect(Collectors.toList()));
    }

    @Override
    public Iterator<Vertex> scanVertices(String... vertexLabels) {
        Set<Integer> vertexLabelIdSet = Sets.newHashSet();
        for (String vertexLabel : vertexLabels) {
            vertexLabelIdSet.add(snapshotSchema.getSchema().getSchemaElement(vertexLabel).getLabelId());
        }
        Map<Integer, Map<Long, Map<String, Object>>> resultVertexList = this.graph.scanVertices(vertexLabelIdSet);
        List<Vertex> vertexList = Lists.newArrayList();
        for (Map.Entry<Integer, Map<Long, Map<String, Object>>> labelEntry : resultVertexList.entrySet()) {
            for (Map.Entry<Long, Map<String, Object>> vertexEntry : labelEntry.getValue().entrySet()) {
                vertexList.add(new MaxGraphVertex(this.maxTinkerGraph,
                        new CompositeId(vertexEntry.getKey(), labelEntry.getKey()),
                        snapshotSchema.getSchema().getSchemaElement(labelEntry.getKey()).getLabel(),
                        vertexEntry.getValue()));
            }
        }

        return vertexList.iterator();
    }

    @Override
    public Iterator<Edge> getEdges(Set<ElementId> edgeIdList) {
        List<Edge> resultEdgeList = Lists.newArrayList();
        edgeIdList.forEach(e -> {
            try {
                resultEdgeList.add(convertMaxEdge(snapshotSchema.getSchema(), this.graph.getEdge(e)));
            } catch (Exception ignored) {
            }
        });
        return resultEdgeList.iterator();
    }

    private MaxGraphEdge convertMaxEdge(GraphSchema schema, DefaultMemoryGraph.DefaultMemoryEdge edge) {
        ElementId srcId = edge.getSrcId();
        MaxGraphVertex srcVertex = new MaxGraphVertex(this.maxTinkerGraph, srcId, schema.getSchemaElement(srcId.labelId()).getLabel());
        ElementId destId = edge.getDestId();
        MaxGraphVertex destVertex = new MaxGraphVertex(this.maxTinkerGraph, destId, schema.getSchemaElement(destId.labelId()).getLabel());
        return new MaxGraphEdge(this.maxTinkerGraph,
                srcVertex,
                destVertex,
                edge.getEdgeId(),
                schema.getSchemaElement(edge.getEdgeId().labelId()).getLabel(),
                edge.getProperties());
    }

    @Override
    public Iterator<Edge> getEdges(ElementId vertexId, Direction direction, String... edgeLabels) {
        if (edgeLabels.length == 0) {
            switch (direction) {
                case OUT: {
                    List<Edge> list = this.graph.getOutEdges(vertexId)
                            .stream()
                            .map(e -> convertMaxEdge(snapshotSchema.getSchema(), e))
                            .collect(Collectors.toList());
                    return list.iterator();
                }
                case IN: {
                    List<Edge> list = this.graph.getInEdges(vertexId)
                            .stream()
                            .map(e -> convertMaxEdge(snapshotSchema.getSchema(), e))
                            .collect(Collectors.toList());
                    return list.iterator();
                }
                case BOTH: {
                    List<Edge> list = this.graph.getOutEdges(vertexId)
                            .stream()
                            .map(e -> convertMaxEdge(snapshotSchema.getSchema(), e))
                            .collect(Collectors.toList());
                    list.addAll(this.graph.getInEdges(vertexId)
                            .stream()
                            .map(e -> convertMaxEdge(snapshotSchema.getSchema(), e))
                            .collect(Collectors.toList()));
                    return list.iterator();
                }
                default: {
                    throw new GraphQueryDataException("Unsupport direction " + direction);
                }
            }
        } else {
            Set<Integer> edgeLabelIdSet = Sets.newHashSet();
            for (String edgeLabel : edgeLabels) {
                edgeLabelIdSet.add(snapshotSchema.getSchema().getSchemaElement(edgeLabel).getLabelId());
            }
            switch (direction) {
                case OUT: {
                    List<Edge> list = this.graph.getOutEdges(vertexId, edgeLabelIdSet)
                            .stream()
                            .map(e -> convertMaxEdge(snapshotSchema.getSchema(), e))
                            .collect(Collectors.toList());
                    return list.iterator();
                }
                case IN: {
                    List<Edge> list = this.graph.getInEdges(vertexId, edgeLabelIdSet)
                            .stream()
                            .map(e -> convertMaxEdge(snapshotSchema.getSchema(), e))
                            .collect(Collectors.toList());
                    return list.iterator();
                }
                case BOTH: {
                    List<Edge> list = this.graph.getOutEdges(vertexId, edgeLabelIdSet)
                            .stream()
                            .map(e -> convertMaxEdge(snapshotSchema.getSchema(), e))
                            .collect(Collectors.toList());
                    list.addAll(this.graph.getInEdges(vertexId, edgeLabelIdSet)
                            .stream()
                            .map(e -> convertMaxEdge(snapshotSchema.getSchema(), e))
                            .collect(Collectors.toList()));
                    return list.iterator();
                }
                default: {
                    throw new GraphQueryDataException("Unsupport direction " + direction);
                }
            }
        }
    }

    @Override
    public Iterator<Edge> scanEdges(String... edgeLabels) {
        Set<Integer> edgeLabelIdSet = Sets.newHashSet();
        for (String edgeLabel : edgeLabels) {
            edgeLabelIdSet.add(snapshotSchema.getSchema().getSchemaElement(edgeLabel).getLabelId());
        }
        List<Edge> edgeList = this.graph.scanEdges(edgeLabelIdSet)
                .stream()
                .map(e -> convertMaxEdge(snapshotSchema.getSchema(), e))
                .collect(Collectors.toList());
        return edgeList.iterator();
    }
}
