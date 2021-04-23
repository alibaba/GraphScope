package com.alibaba.maxgraph.v2.frontend.compiler.cost.statistics;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeRelation;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaElement;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.EdgeOtherVertexTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.EdgeTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.EdgeVertexTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.NodeType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.VertexTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.source.SourceTreeNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class CostDataStatistics {
    private static final String VERTEX_QUERY = "g.estimateVCount('%s')";
    private static final String EDGE_QUERY = "g.estimateECount('%s')";

    private static final double FILTER_RATIO = 0.01;
    private static final double FLATMAP_RATIO = 2.0;

    private static final double INIT_VERTEX_COUNT = 100;
    private static final double INIT_EDGE_COUNT = 10000;

    private Map<String, Double> vertexCountList = Maps.newConcurrentMap();
    private Map<String, Double> edgeCountList = Maps.newConcurrentMap();
    private SchemaFetcher schemaFetcher = null;
    private AtomicBoolean initFlag = new AtomicBoolean(false);

    private static final CostDataStatistics INSTANCE = new CostDataStatistics();

    private CostDataStatistics() {

    }

    public static synchronized void initialize(SchemaFetcher schemaFetcher) {
        INSTANCE.schemaFetcher = schemaFetcher;
    }

    public static CostDataStatistics getInstance() {
        if (null == INSTANCE.schemaFetcher) {
            throw new IllegalArgumentException("statistics cant be used before initialized");
        }
        INSTANCE.initIfNeed();

        return INSTANCE;
    }

    private void initIfNeed() {
        if (!this.initFlag.get()) {
            synchronized (this) {
                if (!this.initFlag.get()) {
                    GraphSchema schema = schemaFetcher.fetchSchema().getSchema();
                    schema.getVertexTypes()
                            .forEach(v -> INSTANCE.vertexCountList.put(v.getLabel(), INIT_VERTEX_COUNT));
                    schema.getEdgeTypes()
                            .forEach(v -> INSTANCE.edgeCountList.put(v.getLabel(), INIT_EDGE_COUNT));
                }
                this.initFlag.set(true);
            }
        }
    }

    public Map<String, String> vertexQueryList() {
        GraphSchema schema = schemaFetcher.fetchSchema().getSchema();
        Map<String, String> queryList = Maps.newHashMap();
        schema.getVertexTypes().forEach(v -> {
            queryList.put(v.getLabel(), String.format(VERTEX_QUERY, v.getLabel()));
        });

        return queryList;
    }

    public Map<String, String> edgeQueryList() {
        GraphSchema schema = schemaFetcher.fetchSchema().getSchema();
        Map<String, String> queryList = Maps.newHashMap();
        schema.getEdgeTypes().forEach(v -> {
            queryList.put(v.getLabel(), String.format(EDGE_QUERY, v.getLabel()));
        });

        return queryList;
    }

    public void addVertexCount(String vertexLabel, double count) {
        this.vertexCountList.put(vertexLabel, count);
    }

    public void addEdgeCount(String edgeLabel, double count) {
        this.edgeCountList.put(edgeLabel, count);
    }

    private double getVertexCount(String label) {
        return this.vertexCountList.getOrDefault(label, INIT_VERTEX_COUNT);
    }

    private double getEdgeCount(String label) {
        return this.edgeCountList.getOrDefault(label, INIT_EDGE_COUNT);
    }

    /**
     * Compute out scale ratio with start vertex label list and edge list
     *
     * @param input    The given start vertex label list
     * @param edgeList The given edge label list
     * @return The result ratio
     */
    public NodeStatistics getOutRatio(NodeStatistics input, Set<String> edgeList) {
        return getDirectionRatio(input, edgeList, true);
    }

    /**
     * Compute in scale ratio with start vertex label list and edge list
     *
     * @param input    The given start vertex label list
     * @param edgeList The given edge label list
     * @return The result ratio
     */
    public NodeStatistics getInRatio(NodeStatistics input, Set<String> edgeList) {
        return getDirectionRatio(input, edgeList, false);
    }

    /**
     * Compute both scale ratio with start vertex label list and edge list
     *
     * @param input    The given start vertex label list
     * @param edgeList The given edge label list
     * @return The result ratio
     */
    public NodeStatistics getBothRatio(NodeStatistics input, Set<String> edgeList) {
        return getOutRatio(input, edgeList).merge(getInRatio(input, edgeList));
    }


    private NodeStatistics getDirectionRatio(NodeStatistics input, Set<String> edgeList, boolean outDirection) {
        GraphSchema schema = schemaFetcher.fetchSchema().getSchema();
        if (schema.getEdgeTypes().isEmpty()) {
            return new NodeStatistics(schema);
        }

        Set<String> currEdgeList;
        if (edgeList == null || edgeList.isEmpty()) {
            currEdgeList = schema.getEdgeTypes().stream().map(SchemaElement::getLabel).collect(Collectors.toSet());
        } else {
            currEdgeList = Sets.newHashSet(edgeList);
        }

        NodeStatistics nodeStatistics = new NodeStatistics(schema);
        Map<String, Double> vertexCountList = input.getVertexCountList();
        currEdgeList.stream()
                .map(v -> {
                    try {
                        return schema.getSchemaElement(v);
                    } catch (Exception ignored) {
                        return null;
                    }
                })
                .filter(v -> v != null && v instanceof EdgeType)
                .map(v -> (EdgeType) v)
                .filter(v -> v.getRelationList().size() > 0)
                .forEach(v -> {
                    double avgRelationRatio = 1.0 / v.getRelationList().size();
                    v.getRelationList()
                            .stream()
                            .filter(vv ->
                                    (outDirection && vertexCountList.containsKey(vv.getSource().getLabel())) ||
                                            ((!outDirection) && vertexCountList.containsKey(vv.getTarget().getLabel())))
                            .forEach(vv -> {
                                if (outDirection) {
                                    Double edgeCount = this.edgeCountList.getOrDefault(v.getLabel(), INIT_EDGE_COUNT);
                                    Double sourceVertexCount = this.vertexCountList.getOrDefault(vv.getSource().getLabel(), INIT_VERTEX_COUNT);
                                    Double currSourceVertexCount = vertexCountList.getOrDefault(vv.getSource().getLabel(), INIT_VERTEX_COUNT);
                                    nodeStatistics.addVertexCount(vv.getTarget().getLabel(),
                                            (edgeCount * avgRelationRatio / sourceVertexCount) * currSourceVertexCount);
                                } else {
                                    nodeStatistics.addVertexCount(vv.getSource().getLabel(),
                                            (this.edgeCountList.getOrDefault(v.getLabel(), INIT_EDGE_COUNT) * avgRelationRatio / this.vertexCountList.getOrDefault(vv.getTarget().getLabel(), INIT_VERTEX_COUNT)) * vertexCountList.getOrDefault(vv.getTarget().getLabel(), INIT_VERTEX_COUNT));
                                }
                            });
                });

        return nodeStatistics;
    }

    /**
     * Compute outE scale ratio with start vertex label list and edge list
     *
     * @param input    The given start vertex label list
     * @param edgeList The given edge label list
     * @return The result ratio
     */
    public NodeStatistics getOutERatio(NodeStatistics input, Set<String> edgeList) {
        GraphSchema schema = schemaFetcher.fetchSchema().getSchema();
        NodeStatistics nodeStatistics = new NodeStatistics(schema);
        Map<String, Double> inputVertexCountList = input.getVertexCountList();
        for (String edgeLabel : edgeList) {
            try {
                EdgeType edge = (EdgeType) schema.getSchemaElement(edgeLabel);
                List<EdgeRelation> relationList = edge.getRelationList();
                if (relationList.size() > 0) {
                    double avgRelationRatio = 1.0 / edge.getRelationList().size();
                    relationList.stream()
                            .filter(v -> inputVertexCountList.containsKey(v.getSource().getLabel()))
                            .forEach(v -> {
                                double sourceVertexCount = inputVertexCountList.getOrDefault(v.getSource().getLabel(), INIT_VERTEX_COUNT);
                                double currEdgeCount = (this.edgeCountList.getOrDefault(edge.getLabel(), INIT_EDGE_COUNT) * avgRelationRatio / this.vertexCountList.getOrDefault(v.getSource().getLabel(), INIT_VERTEX_COUNT))
                                        * sourceVertexCount;
                                nodeStatistics.addEdgeCount(edgeLabel, currEdgeCount);
                            });
                }
            } catch (Exception ignored) {
            }
        }

        return nodeStatistics;
    }

    /**
     * Compute inE scale ratio with start vertex label list and edge list
     *
     * @param input    The given start vertex label list
     * @param edgeList The given edge label list
     * @return The result ratio
     */
    public NodeStatistics getInERatio(NodeStatistics input, Set<String> edgeList) {
        GraphSchema schema = schemaFetcher.fetchSchema().getSchema();
        NodeStatistics nodeStatistics = new NodeStatistics(schema);
        Map<String, Double> inputVertexCountList = input.getVertexCountList();
        for (String edgeLabel : edgeList) {
            try {
                EdgeType edge = (EdgeType) schema.getSchemaElement(edgeLabel);
                List<EdgeRelation> relationList = edge.getRelationList();
                if (relationList.size() > 0) {
                    double avgRelationRatio = 1.0 / edge.getRelationList().size();
                    relationList.stream()
                            .filter(v -> inputVertexCountList.containsKey(v.getTarget().getLabel()))
                            .forEach(v -> {
                                double sourceVertexCount = inputVertexCountList.getOrDefault(v.getTarget().getLabel(), INIT_VERTEX_COUNT);
                                double currEdgeCount = (this.edgeCountList.getOrDefault(edge.getLabel(), INIT_EDGE_COUNT) * avgRelationRatio / this.vertexCountList.getOrDefault(v.getTarget().getLabel(), INIT_VERTEX_COUNT))
                                        * sourceVertexCount;
                                nodeStatistics.addEdgeCount(edgeLabel, currEdgeCount);
                            });
                }
            } catch (Exception ignored) {
            }
        }

        return nodeStatistics;
    }

    /**
     * Compute bothE scale ratio with start vertex label list and edge list
     *
     * @param input    The given start vertex label list
     * @param edgeList The given edge label list
     * @return The result ratio
     */
    public NodeStatistics getBothERatio(NodeStatistics input, Set<String> edgeList) {
        NodeStatistics nodeStatistics = new NodeStatistics(schemaFetcher.fetchSchema().getSchema());
        nodeStatistics.merge(getOutRatio(input, edgeList));
        nodeStatistics.merge(getInRatio(input, edgeList));

        return nodeStatistics;
    }

    /**
     * Compute step count list with given node label manager and node list
     *
     * @param nodeLabelManager The given node label manager
     * @param treeNodeList     The given node list
     * @return The result step count list
     */
    public List<Double> computeStepCountList(NodeLabelManager nodeLabelManager, List<TreeNode> treeNodeList) {
        List<NodeStatistics> nodeStatisticsList = Lists.newArrayList();
        List<NodeLabelList> nodeLabelLists = nodeLabelManager.getNodeLabelList();
        for (int i = 0; i < nodeLabelLists.size(); i++) {
            NodeStatistics inputStatistics = i == 0 ? null : nodeStatisticsList.get(i - 1);
            NodeLabelList currNodeLabel = nodeLabelLists.get(i);
            TreeNode treeNode = treeNodeList.get(i);
            nodeStatisticsList.add(computeStepCount(inputStatistics, treeNode, currNodeLabel));
        }

        return nodeStatisticsList.stream().map(NodeStatistics::totalCount).collect(Collectors.toList());
    }

    /**
     * Compute step count with given node and node label
     *
     * @param inputStatistics The given input statistics
     * @param treeNode        The given tree node
     * @param currNodeLabel   The given current node label
     * @return The result step count
     */
    private NodeStatistics computeStepCount(NodeStatistics inputStatistics,
                                            TreeNode treeNode,
                                            NodeLabelList currNodeLabel) {
        GraphSchema schema = schemaFetcher.fetchSchema().getSchema();
        NodeStatistics nodeStatistics = new NodeStatistics(schema);
        if (null == inputStatistics) {
            if (currNodeLabel.isUnknownFlag()) {
                for (Map.Entry<String, Double> entry : vertexCountList.entrySet()) {
                    nodeStatistics.addVertexCount(entry.getKey(), entry.getValue());
                }
            } else {
                SourceTreeNode sourceTreeNode = (SourceTreeNode) treeNode;
                Object[] ids = sourceTreeNode.getIds();
                Set<String> vertexLabelList = currNodeLabel.getVertexLabelList();
                for (String vertexLabel : vertexLabelList) {
                    double vertexCount = null == ids ? vertexCountList.getOrDefault(vertexLabel, INIT_VERTEX_COUNT) : (ids.length * 1.0 / vertexLabelList.size());
                    nodeStatistics.addVertexCount(vertexLabel, vertexCount);
                }
                Set<String> edgeLabelList = currNodeLabel.getEdgeLabelList();
                for (String edgeLabel : currNodeLabel.getEdgeLabelList()) {
                    double edgeCount = null == ids ? edgeCountList.getOrDefault(edgeLabel, INIT_EDGE_COUNT) : (ids.length * 1.0 / edgeLabelList.size());
                    nodeStatistics.addEdgeCount(edgeLabel, edgeCount);
                }
            }
        } else {
            if (treeNode instanceof VertexTreeNode) {
                VertexTreeNode vertexTreeNode = (VertexTreeNode) treeNode;
                Direction direction = vertexTreeNode.getDirection();
                Set<String> edgeLabelList;
                if (null == vertexTreeNode.getEdgeLabels()) {
                    edgeLabelList = Sets.newHashSet();
                } else {
                    edgeLabelList = Sets.newHashSet(vertexTreeNode.getEdgeLabels());
                }
                switch (direction) {
                    case OUT: {
                        nodeStatistics.merge(getOutRatio(inputStatistics, edgeLabelList));
                        break;
                    }
                    case IN: {
                        nodeStatistics.merge(getInRatio(inputStatistics, edgeLabelList));
                        break;
                    }
                    case BOTH: {
                        nodeStatistics.merge(getOutRatio(inputStatistics, edgeLabelList));
                        nodeStatistics.merge(getInRatio(inputStatistics, edgeLabelList));
                        break;
                    }
                }
            } else if (treeNode instanceof EdgeTreeNode) {
                Set<String> edgeLabelList = currNodeLabel.getEdgeLabelList();
                Direction direction = ((EdgeTreeNode) treeNode).getDirection();
                switch (direction) {
                    case OUT: {
                        nodeStatistics.merge(getOutERatio(inputStatistics, edgeLabelList));
                        break;
                    }
                    case IN: {
                        nodeStatistics.merge(getInERatio(inputStatistics, edgeLabelList));
                        break;
                    }
                    case BOTH: {
                        nodeStatistics.merge(getOutERatio(inputStatistics, edgeLabelList));
                        nodeStatistics.merge(getInERatio(inputStatistics, edgeLabelList));
                        break;
                    }
                }
            } else if (treeNode instanceof EdgeVertexTreeNode) {
                Map<String, Double> edgeCountList = inputStatistics.getEdgeCountList();
                Direction direction = ((EdgeVertexTreeNode) treeNode).getDirection();
                edgeCountList.keySet().stream()
                        .map(v -> {
                            try {
                                return schema.getSchemaElement(v);
                            } catch (Exception ignored) {
                                return null;
                            }
                        })
                        .filter(v -> null != v && v instanceof EdgeType)
                        .map(v -> (EdgeType) v)
                        .forEach(v -> {
                            v.getRelationList()
                                    .forEach(vv -> {
                                        double currEdgeCount = edgeCountList.get(v.getLabel());
                                        if (direction == Direction.OUT || direction == Direction.BOTH) {
                                            List<String> resultVertexLabelList = Lists.newArrayList();
                                            if (currNodeLabel.getVertexLabelList().contains(vv.getSource().getLabel())) {
                                                resultVertexLabelList.add(vv.getSource().getLabel());
                                            }
                                            double avgVertexCount = currEdgeCount / resultVertexLabelList.size();
                                            for (String vertexLabel : resultVertexLabelList) {
                                                nodeStatistics.addVertexCount(vertexLabel, avgVertexCount);
                                            }
                                        }
                                        if (direction == Direction.IN || direction == Direction.BOTH) {
                                            List<String> resultVertexLabelList = Lists.newArrayList();
                                            if (currNodeLabel.getVertexLabelList().contains(vv.getTarget().getLabel())) {
                                                resultVertexLabelList.add(vv.getTarget().getLabel());
                                            }
                                            double avgVertexCount = currEdgeCount / resultVertexLabelList.size();
                                            for (String vertexLabel : resultVertexLabelList) {
                                                nodeStatistics.addVertexCount(vertexLabel, avgVertexCount);
                                            }
                                        }
                                    });

                        });
            } else if (treeNode instanceof EdgeOtherVertexTreeNode) {
                Set<String> vertexLabelList = currNodeLabel.getVertexLabelList();
                double avgVertexCount = inputStatistics.totalCount() / vertexLabelList.size();
                vertexLabelList.forEach(v -> nodeStatistics.addVertexCount(v, avgVertexCount));
            } else {
                NodeType nodeType = treeNode.getNodeType();
                if (NodeType.MAP == nodeType) {
                    nodeStatistics.addElementCount(inputStatistics.totalCount());
                } else if (NodeType.FILTER == nodeType) {
                    nodeStatistics.merge(inputStatistics, FILTER_RATIO);
                } else if (NodeType.FLATMAP == nodeType) {
                    nodeStatistics.addElementCount(inputStatistics.totalCount() * FLATMAP_RATIO);
                } else if (NodeType.AGGREGATE == nodeType) {
                    nodeStatistics.addElementCount(1);
                } else {
                    nodeStatistics.merge(inputStatistics);
                }
            }
        }

        return nodeStatistics;
    }
}
