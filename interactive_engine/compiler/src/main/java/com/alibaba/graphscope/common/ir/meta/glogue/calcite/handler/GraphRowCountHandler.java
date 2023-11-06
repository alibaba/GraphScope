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

import com.alibaba.graphscope.common.ir.meta.glogue.Utils;
import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendStep;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueQuery;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.google.common.base.Preconditions;
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

class GraphRowCountHandler implements BuiltInMetadata.RowCount.Handler {
    private final RelMdRowCount mdRowCount;
    private final GlogueQuery glogueQuery;
    private final RelOptPlanner optPlanner;

    public GraphRowCountHandler(
            RelOptPlanner optPlanner, RelMdRowCount mdRowCount, GlogueQuery glogueQuery) {
        this.optPlanner = optPlanner;
        this.mdRowCount = mdRowCount;
        this.glogueQuery = glogueQuery;
    }

    @Override
    public Double getRowCount(RelNode node, RelMetadataQuery mq) {
        if (node instanceof GraphPattern) {
            Pattern pattern = ((GraphPattern) node).getPattern();
            int patternSize = pattern.getVertexNumber();
            // todo: estimate the pattern graph with filter conditions
            if (patternSize <= glogueQuery.getMaxPatternSize()) {
                return glogueQuery.getRowCount(((GraphPattern) node).getPattern());
            }
            // estimate the pattern graph with intersect, i.e. a->b, c->b, d->b
            PatternVertex intersect = getIntersectVertex(pattern);
            if (intersect != null) {
                Set<PatternEdge> edges = pattern.getEdgesOf(intersect);
                Preconditions.checkArgument(
                        !edges.isEmpty(), "intersect vertex should have at least one edge");
                double count = 1.0;
                for (PatternEdge edge : edges) {
                    Pattern edgePattern = new Pattern();
                    edgePattern.addVertex(edge.getSrcVertex());
                    edgePattern.addVertex(edge.getDstVertex());
                    edgePattern.addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
                    count *= glogueQuery.getRowCount(edgePattern);
                }
                double intersectCount = glogueQuery.getRowCount(new Pattern(intersect));
                count /= Math.pow(intersectCount, edges.size() - 1);
                return count;
            } else {
                // try to estimate count based on existed partitions by rules
                if (optPlanner instanceof VolcanoPlanner) {
                    RelSubset subset = ((VolcanoPlanner) optPlanner).getSubset(node);
                    if (subset != null) {
                        GraphExtendIntersect extendIntersect =
                                (GraphExtendIntersect)
                                        feasibleIntersects(subset);
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
                        double count =
                                getRowCount(subGraphPattern(extendIntersect), mq)
                                        * getRowCount(
                                                new GraphPattern(
                                                        node.getCluster(),
                                                        node.getTraitSet(),
                                                        extendPattern),
                                                mq);
                        for (PatternVertex vertex : extendFromVertices) {
                            count /= glogueQuery.getRowCount(new Pattern(vertex));
                        }
                        return count;
                    }
                }
            }
            throw new UnsupportedOperationException(
                    "estimate count for pattern " + pattern + " is unsupported yet");
        } else if (node instanceof Filter) {
            return mdRowCount.getRowCount((Filter) node, mq);
        } else if (node instanceof Aggregate) {
            return mdRowCount.getRowCount((Aggregate) node, mq);
        } else if (node instanceof Sort) {
            return mdRowCount.getRowCount((Sort) node, mq);
        } else if (node instanceof Project) {
            return mdRowCount.getRowCount((Project) node, mq);
        } else if (node instanceof RelSubset) {
            return mdRowCount.getRowCount((RelSubset) node, mq);
        } else if (node instanceof Join) {
            return mdRowCount.getRowCount((Join) node, mq);
        } else if (node instanceof Union) {
            return mdRowCount.getRowCount((Union) node, mq);
        } else if (node instanceof GraphExtendIntersect) {
            if (optPlanner instanceof VolcanoPlanner) {
                RelSubset subset = ((VolcanoPlanner) optPlanner).getSubset(node);
                if (subset != null) {
                    RelNode currentPattern = subset.getOriginal();
                    // use the row count of the current pattern to estimate the communication cost
                    return mq.getRowCount(currentPattern);
                }
            }
        }
        throw new IllegalArgumentException("can not estimate row count for the node=" + node);
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

    private @Nullable PatternVertex getIntersectVertex(Pattern pattern) {
        int edgeNum = pattern.getEdgeNumber();
        if (edgeNum == 0) return null;
        for (PatternVertex vertex : pattern.getVertexSet()) {
            if (pattern.getDegree(vertex) == edgeNum) {
                return vertex;
            }
        }
        return null;
    }

    private @Nullable RelNode subGraphPattern(GraphExtendIntersect intersect) {
        RelNode input = intersect.getInput(0);
        return (input instanceof RelSubset) ? ((RelSubset) input).getOriginal() : input;
    }
}
