/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.meta.glogue.calcite.handler;

import com.alibaba.graphscope.common.ir.meta.glogue.PrimitiveCountEstimator;
import com.alibaba.graphscope.common.ir.meta.glogue.Utils;
import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphJoinDecomposition;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendStep;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueQuery;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class GraphRowCountHandler implements BuiltInMetadata.RowCount.Handler {
    private final PrimitiveCountEstimator countEstimator;
    private final RelOptPlanner optPlanner;
    private final RelMdRowCount mdRowCount;

    public GraphRowCountHandler(RelOptPlanner optPlanner, GlogueQuery glogueQuery) {
        this.optPlanner = optPlanner;
        this.countEstimator = new PrimitiveCountEstimator(glogueQuery);
        this.mdRowCount = new RelMdRowCount();
    }

    @Override
    public Double getRowCount(RelNode node, RelMetadataQuery mq) {
        if (node instanceof GraphPattern) {
            Pattern pattern = ((GraphPattern) node).getPattern();
            Double countEstimate = countEstimator.estimate(pattern);
            if (countEstimate != null) {
                return countEstimate;
            }
            // try to estimate count based on existed partitions by rules
            if (optPlanner instanceof VolcanoPlanner) {
                RelSubset subset = ((VolcanoPlanner) optPlanner).getSubset(node);
                if (subset != null) {
                    GraphExtendIntersect extendIntersect =
                            (GraphExtendIntersect) feasibleIntersects(subset);
                    if (extendIntersect != null) {
                        ExtendStep extendStep = extendIntersect.getGlogueEdge().getExtendStep();
                        int targetOrder = extendStep.getTargetVertexOrder();
                        PatternVertex target = pattern.getVertexByOrder(targetOrder);
                        Set<PatternEdge> adjacentEdges = pattern.getEdgesOf(target);
                        Pattern extendPattern = new Pattern();
                        List<PatternVertex> extendFromVertices = Lists.newArrayList();
                        for (PatternEdge edge : adjacentEdges) {
                            extendPattern.addVertex(edge.getSrcVertex());
                            extendPattern.addVertex(edge.getDstVertex());
                            extendPattern.addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
                            extendFromVertices.add(Utils.getExtendFromVertex(edge, target));
                        }
                        return getRowCount(
                                (GraphPattern) subGraphPattern(extendIntersect, 0),
                                new GraphPattern(
                                        node.getCluster(), node.getTraitSet(), extendPattern),
                                extendFromVertices,
                                mq);
                    }
                    GraphJoinDecomposition joinDecomposition =
                            (GraphJoinDecomposition) feasibleJoinDecomposition(subset);
                    if (joinDecomposition != null) {
                        Pattern buildPattern = joinDecomposition.getBuildPattern();
                        List<PatternVertex> jointVertices =
                                joinDecomposition.getJoinVertexPairs().stream()
                                        .map(
                                                k ->
                                                        buildPattern.getVertexByOrder(
                                                                k.getRightOrderId()))
                                        .collect(Collectors.toList());
                        return getRowCount(
                                (GraphPattern) subGraphPattern(joinDecomposition, 0),
                                (GraphPattern) subGraphPattern(joinDecomposition, 1),
                                jointVertices,
                                mq);
                    }
                }
            }
            double totalRowCount = 1.0d;
            for (PatternEdge edge : pattern.getEdgeSet()) {
                totalRowCount *= countEstimator.estimate(edge);
            }
            for (PatternVertex vertex : pattern.getVertexSet()) {
                int degree = pattern.getEdgesOf(vertex).size();
                if (degree > 0) {
                    double vertexCount = countEstimator.estimate(vertex);
                    vertexCount = Double.compare(vertexCount, 0.0d) == 0 ? 1.0d : vertexCount;
                    totalRowCount /= Math.pow(vertexCount, degree - 1);
                }
            }
            return totalRowCount;
        } else if (node instanceof TableScan) {
            return getRowCount((TableScan) node, mq);
        } else if (node instanceof Filter) {
            return mdRowCount.getRowCount((Filter) node, mq);
        } else if (node instanceof Aggregate) {
            return mdRowCount.getRowCount((Aggregate) node, mq);
        } else if (node instanceof Sort) {
            return mdRowCount.getRowCount((Sort) node, mq);
        } else if (node instanceof Project) {
            return mdRowCount.getRowCount((Project) node, mq);
        } else if (node instanceof RelSubset) {
            return mq.getRowCount(((RelSubset) node).getOriginal());
        } else if (node instanceof GraphExtendIntersect || node instanceof GraphJoinDecomposition) {
            if (optPlanner instanceof VolcanoPlanner) {
                RelSubset subset = ((VolcanoPlanner) optPlanner).getSubset(node);
                if (subset != null) {
                    // use the row count of the current pattern to estimate the communication cost
                    return mq.getRowCount(subset);
                }
            }
            Pattern original =
                    (node instanceof GraphExtendIntersect)
                            ? ((GraphExtendIntersect) node).getGlogueEdge().getDstPattern()
                            : ((GraphJoinDecomposition) node).getParentPatten();
            return mq.getRowCount(
                    new GraphPattern(node.getCluster(), node.getTraitSet(), original));
        } else if (node instanceof Join) {
            return mdRowCount.getRowCount((Join) node, mq);
        } else if (node instanceof Union) {
            return mdRowCount.getRowCount((Union) node, mq);
        }
        throw new IllegalArgumentException("can not estimate row count for the node=" + node);
    }

    private double getRowCount(TableScan rel, RelMetadataQuery mq) {
        if (rel instanceof GraphLogicalSource || rel instanceof GraphLogicalGetV) {
            List<Integer> vertexTypeIds = Utils.getVertexTypeIds(rel);
            PatternVertex vertex =
                    (vertexTypeIds.size() == 1)
                            ? new SinglePatternVertex(vertexTypeIds.get(0))
                            : new FuzzyPatternVertex(vertexTypeIds);
            return mq.getRowCount(
                    new GraphPattern(rel.getCluster(), rel.getTraitSet(), new Pattern(vertex)));
        } else if (rel instanceof GraphLogicalExpand) {
            List<EdgeTypeId> edgeTypeIds = Utils.getEdgeTypeIds(rel);
            List<Integer> srcVertexTypeIds =
                    edgeTypeIds.stream().map(k -> k.getSrcLabelId()).collect(Collectors.toList());
            List<Integer> dstVertexTypeIds =
                    edgeTypeIds.stream().map(k -> k.getDstLabelId()).collect(Collectors.toList());
            PatternVertex srcVertex =
                    (srcVertexTypeIds.size() == 1)
                            ? new SinglePatternVertex(srcVertexTypeIds.get(0), 0)
                            : new FuzzyPatternVertex(srcVertexTypeIds, 0);
            PatternVertex dstVertex =
                    (dstVertexTypeIds.size() == 1)
                            ? new SinglePatternVertex(dstVertexTypeIds.get(0), 1)
                            : new FuzzyPatternVertex(dstVertexTypeIds, 1);
            boolean isBoth = ((GraphLogicalExpand) rel).getOpt() == GraphOpt.Expand.BOTH;
            PatternEdge edge =
                    (edgeTypeIds.size() == 1)
                            ? new SinglePatternEdge(
                                    srcVertex,
                                    dstVertex,
                                    edgeTypeIds.get(0),
                                    0,
                                    isBoth,
                                    new ElementDetails())
                            : new FuzzyPatternEdge(
                                    srcVertex,
                                    dstVertex,
                                    edgeTypeIds,
                                    0,
                                    isBoth,
                                    new ElementDetails());
            Pattern pattern = new Pattern();
            pattern.addVertex(srcVertex);
            pattern.addVertex(dstVertex);
            pattern.addEdge(srcVertex, dstVertex, edge);
            return mq.getRowCount(new GraphPattern(rel.getCluster(), rel.getTraitSet(), pattern));
        }
        throw new IllegalArgumentException("can not estimate row count for the rel=" + rel);
    }

    private double getRowCount(
            GraphPattern p1,
            GraphPattern p2,
            List<PatternVertex> jointVertices,
            RelMetadataQuery mq) {
        double count = getRowCount(p1, mq) * getRowCount(p2, mq);
        for (PatternVertex vertex : jointVertices) {
            count /= countEstimator.estimate(vertex);
        }
        return count;
    }

    private @Nullable RelNode feasibleIntersects(RelSubset subSet) {
        List<RelNode> rels = subSet.getRelList();
        for (RelNode rel : rels) {
            if (rel instanceof GraphExtendIntersect) {
                GraphExtendIntersect intersect1 = (GraphExtendIntersect) rel;
                if (intersect1.getInput(0) instanceof RelSubset) {
                    RelSubset subset1 = (RelSubset) intersect1.getInput(0);
                    if (subset1.getBest() != null) {
                        return rel;
                    }
                }
            }
        }
        return null;
    }

    private @Nullable RelNode subGraphPattern(RelNode rel, int subId) {
        RelNode input = rel.getInput(subId);
        return (input instanceof RelSubset) ? ((RelSubset) input).getOriginal() : input;
    }

    private @Nullable RelNode feasibleJoinDecomposition(RelSubset subSet) {
        List<RelNode> rels = subSet.getRelList();
        for (RelNode rel : rels) {
            if (rel instanceof GraphJoinDecomposition) {
                GraphJoinDecomposition decomposition = (GraphJoinDecomposition) rel;
                if (decomposition.getLeft() instanceof RelSubset
                        && decomposition.getRight() instanceof RelSubset) {
                    RelSubset left = (RelSubset) decomposition.getLeft();
                    RelSubset right = (RelSubset) decomposition.getRight();
                    if (left.getBest() != null && right.getBest() != null) {
                        return rel;
                    }
                }
            }
        }
        return null;
    }
}
