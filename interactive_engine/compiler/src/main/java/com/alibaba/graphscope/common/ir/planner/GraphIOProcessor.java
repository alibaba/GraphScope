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

package com.alibaba.graphscope.common.ir.planner;

import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.glogue.CountHandler;
import com.alibaba.graphscope.common.ir.meta.glogue.DetailedExpandCost;
import com.alibaba.graphscope.common.ir.meta.glogue.DetailedSourceCost;
import com.alibaba.graphscope.common.ir.meta.glogue.EdgeCostEstimator;
import com.alibaba.graphscope.common.ir.meta.glogue.calcite.GraphRelMetadataQuery;
import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.alibaba.graphscope.common.ir.planner.type.DataKey;
import com.alibaba.graphscope.common.ir.planner.type.DataValue;
import com.alibaba.graphscope.common.ir.planner.type.EdgeDataKey;
import com.alibaba.graphscope.common.ir.planner.type.VertexDataKey;
import com.alibaba.graphscope.common.ir.rel.*;
import com.alibaba.graphscope.common.ir.rel.graph.*;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendStep;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueExtendIntersectEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.type.JoinVertexEntry;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.Utils;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphPathType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.groot.common.schema.api.EdgeRelation;
import com.alibaba.graphscope.groot.common.schema.api.GraphEdge;
import com.alibaba.graphscope.groot.common.schema.api.GraphVertex;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVariable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GraphIOProcessor {
    private final GraphBuilder builder;
    private final IrMeta irMeta;
    private final RelMetadataQuery mq;
    private final Map<DataKey, DataValue> graphDetails;

    public GraphIOProcessor(GraphBuilder builder, IrMeta irMeta) {
        this.builder = Objects.requireNonNull(builder);
        this.irMeta = Objects.requireNonNull(irMeta);
        this.mq = builder.getCluster().getMetadataQuery();
        this.graphDetails = Maps.newHashMap();
    }

    /**
     * convert {@code Match} to {@code Pattern}
     * @param input
     * @return
     */
    public RelNode processInput(RelNode input) {
        return processInput(ImmutableList.of(input));
    }

    public RelNode processInput(List<RelNode> inputs) {
        InputConvertor convertor = new InputConvertor();
        RelNode processed = null;
        for (RelNode input : inputs) {
            processed = input.accept(convertor);
        }
        convertor.build();
        return processed;
    }

    /**
     * convert {@code Intersect} to {@code Expand} or {@code MultiJoin}
     * @param output
     * @return
     */
    public RelNode processOutput(RelNode output) {
        return output.accept(new OutputConvertor());
    }

    public GraphBuilder getBuilder() {
        return builder;
    }

    private class InputConvertor extends GraphShuttle {
        private final Map<String, PatternVertex> aliasNameToVertex;
        private final AtomicInteger idGenerator;
        private final Map<Object, DataValue> vertexOrEdgeDetails;
        private final Pattern inputPattern;

        public InputConvertor() {
            this.aliasNameToVertex = Maps.newHashMap();
            this.idGenerator = new AtomicInteger(0);
            this.vertexOrEdgeDetails = Maps.newHashMap();
            this.inputPattern = new Pattern();
            this.inputPattern.setPatternId(UUID.randomUUID().hashCode());
        }

        @Override
        public RelNode visit(GraphLogicalSingleMatch match) {
            return new GraphPattern(
                    match.getCluster(),
                    match.getTraitSet(),
                    visit(
                            ImmutableList.of(match.getSentence()),
                            match.getMatchOpt() == GraphOpt.Match.OPTIONAL));
        }

        @Override
        public RelNode visit(GraphLogicalMultiMatch match) {
            return new GraphPattern(
                    match.getCluster(), match.getTraitSet(), visit(match.getSentences(), false));
        }

        public void build() {
            // set vertex optional: if all edges of a vertex are optional, then the vertex is
            // optional
            inputPattern
                    .getVertexSet()
                    .forEach(
                            k -> {
                                Set<PatternEdge> edges = inputPattern.getEdgesOf(k);
                                if (!edges.isEmpty()
                                        && edges.stream()
                                                .allMatch(
                                                        e -> e.getElementDetails().isOptional())) {
                                    k.getElementDetails().setOptional(true);
                                }
                            });
            inputPattern.reordering();
            checkPattern(inputPattern);

            // set graph details
            if (!graphDetails.isEmpty()) {
                graphDetails.clear();
            }
            if (inputPattern != null && !vertexOrEdgeDetails.isEmpty()) {
                vertexOrEdgeDetails.forEach(
                        (k, v) -> {
                            DataKey key = null;
                            if (k instanceof PatternVertex) {
                                key =
                                        new VertexDataKey(
                                                inputPattern.getVertexOrder((PatternVertex) k));
                            } else if (k instanceof PatternEdge) {
                                int srcOrderId =
                                        inputPattern.getVertexOrder(
                                                ((PatternEdge) k).getSrcVertex());
                                int dstOrderId =
                                        inputPattern.getVertexOrder(
                                                ((PatternEdge) k).getDstVertex());
                                PatternDirection direction =
                                        ((PatternEdge) k).isBoth()
                                                ? PatternDirection.BOTH
                                                : PatternDirection.OUT;
                                key = new EdgeDataKey(srcOrderId, dstOrderId, direction);
                            }
                            graphDetails.put(key, v);
                        });
            }
        }

        private Pattern visit(List<RelNode> sentences, boolean optional) {
            RelVisitor visitor =
                    new RelVisitor() {
                        PatternVertex lastVisited = null;

                        @Override
                        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                            super.visit(node, ordinal, parent);
                            if (node instanceof GraphLogicalSource) {
                                GraphLogicalSource source = (GraphLogicalSource) node;
                                lastVisited = visitAndAddVertex(source);
                            } else if (node instanceof GraphLogicalExpand) {
                                Preconditions.checkArgument(
                                        parent instanceof GraphLogicalGetV,
                                        "there should be a getV operator after expand since edge in"
                                                + " patten should have two endpoints");
                                PatternVertex vertex = visitAndAddVertex((GraphLogicalGetV) parent);
                                visitAndAddEdge((GraphLogicalExpand) node, lastVisited, vertex);
                                lastVisited = vertex;
                            } else if (node instanceof GraphLogicalPathExpand) {
                                Preconditions.checkArgument(
                                        parent instanceof GraphLogicalGetV,
                                        "there should be a getV operator after path expand since"
                                                + " edge in patten should have two endpoints");
                                Preconditions.checkArgument(
                                        ((GraphLogicalPathExpand) node).getUntilCondition() == null,
                                        "cannot apply optimization if path expand has until"
                                                + " conditions");
                                PatternVertex vertex = visitAndAddVertex((GraphLogicalGetV) parent);
                                visitAndAddPxdEdge(
                                        (GraphLogicalPathExpand) node, lastVisited, vertex);
                                lastVisited = vertex;
                            }
                            if (parent != null
                                    && (node instanceof GraphLogicalSource
                                            || node instanceof GraphLogicalGetV)) {
                                DataValue value = vertexOrEdgeDetails.get(lastVisited);
                                if (value != null
                                        && (value.getAlias() == null
                                                || value.getAlias()
                                                        == AliasInference.DEFAULT_NAME)) {
                                    vertexOrEdgeDetails.put(
                                            lastVisited,
                                            new DataValue(
                                                    generateAlias(lastVisited), value.getFilter()));
                                }
                            }
                        }

                        private String generateAlias(PatternVertex vertex) {
                            return "PATTERN_VERTEX$" + vertex.getId();
                        }

                        private PatternVertex visitAndAddVertex(
                                AbstractBindableTableScan tableScan) {
                            String alias = tableScan.getAliasName();
                            PatternVertex existVertex = aliasNameToVertex.get(alias);
                            RexNode filters = getFilters(tableScan);
                            if (existVertex == null) {
                                int vertexId = idGenerator.getAndIncrement();
                                List<Integer> typeIds =
                                        com.alibaba.graphscope.common.ir.meta.glogue.Utils
                                                .getVertexTypeIds(tableScan);
                                double selectivity = mq.getSelectivity(tableScan, filters);
                                existVertex =
                                        (typeIds.size() == 1)
                                                ? new SinglePatternVertex(
                                                        typeIds.get(0),
                                                        vertexId,
                                                        new ElementDetails(selectivity))
                                                : new FuzzyPatternVertex(
                                                        typeIds,
                                                        vertexId,
                                                        new ElementDetails(selectivity));
                                inputPattern.addVertex(existVertex);
                                if (alias != AliasInference.DEFAULT_NAME) {
                                    aliasNameToVertex.put(alias, existVertex);
                                }
                                vertexOrEdgeDetails.put(existVertex, new DataValue(alias, filters));
                            } else if (filters != null) {
                                DataValue value = vertexOrEdgeDetails.get(existVertex);
                                // reset condition
                                RexNode newCondition =
                                        (value.getFilter() == null)
                                                ? filters
                                                : RexUtil.composeConjunction(
                                                        builder.getRexBuilder(),
                                                        ImmutableList.of(
                                                                filters, value.getFilter()));
                                vertexOrEdgeDetails.put(
                                        existVertex,
                                        new DataValue(
                                                value.getAlias(),
                                                newCondition,
                                                value.getParentAlias()));
                            }
                            return existVertex;
                        }

                        private PatternEdge visitAndAddEdge(
                                GraphLogicalExpand expand,
                                PatternVertex left,
                                PatternVertex right) {
                            PatternVertex src, dst;
                            switch (expand.getOpt()) {
                                case OUT:
                                case BOTH:
                                    src = left;
                                    dst = right;
                                    break;
                                case IN:
                                default:
                                    src = right;
                                    dst = left;
                            }
                            PatternEdge edge = visitEdge(expand, src, dst);
                            boolean added = inputPattern.addEdge(src, dst, edge);
                            if (!added) {
                                throw new UnsupportedOperationException(
                                        "edge "
                                                + edge
                                                + " already exists in the pattern, and pattern with"
                                                + " multi-edges are not supported yet");
                            }

                            vertexOrEdgeDetails.put(
                                    edge, new DataValue(expand.getAliasName(), getFilters(expand)));
                            return edge;
                        }

                        private PatternEdge visitAndAddPxdEdge(
                                GraphLogicalPathExpand pxd,
                                PatternVertex left,
                                PatternVertex right) {
                            GraphLogicalExpand expand = (GraphLogicalExpand) pxd.getExpand();
                            PatternVertex src, dst;
                            switch (expand.getOpt()) {
                                case OUT:
                                case BOTH:
                                    src = left;
                                    dst = right;
                                    break;
                                case IN:
                                default:
                                    src = right;
                                    dst = left;
                            }
                            PatternEdge expandEdge = visitEdge(expand, src, dst);
                            int offset =
                                    (pxd.getOffset() == null)
                                            ? 0
                                            : ((RexLiteral) pxd.getOffset())
                                                    .getValueAs(Number.class)
                                                    .intValue();
                            int fetch =
                                    (pxd.getFetch() == null)
                                            ? (Integer.MAX_VALUE - offset)
                                            : ((RexLiteral) pxd.getFetch())
                                                    .getValueAs(Number.class)
                                                    .intValue();
                            GraphPathType pathType =
                                    (GraphPathType)
                                            pxd.getRowType().getFieldList().get(0).getType();
                            GraphLabelType labelType =
                                    ((GraphSchemaType) pathType.getComponentType().getGetVType())
                                            .getLabelType();
                            List<Integer> innerGetVTypes =
                                    labelType.getLabelsEntry().stream()
                                            .map(k -> k.getLabelId())
                                            .collect(Collectors.toList());
                            ElementDetails newDetails =
                                    new ElementDetails(
                                            expandEdge.getElementDetails().getSelectivity(),
                                            new PathExpandRange(offset, fetch),
                                            innerGetVTypes,
                                            pxd.getResultOpt(),
                                            pxd.getPathOpt(),
                                            expandEdge.getElementDetails().isOptional());
                            expandEdge =
                                    (expandEdge instanceof SinglePatternEdge)
                                            ? new SinglePatternEdge(
                                                    expandEdge.getSrcVertex(),
                                                    expandEdge.getDstVertex(),
                                                    expandEdge.getEdgeTypeIds().get(0),
                                                    expandEdge.getId(),
                                                    expandEdge.isBoth(),
                                                    newDetails)
                                            : new FuzzyPatternEdge(
                                                    expandEdge.getSrcVertex(),
                                                    expandEdge.getDstVertex(),
                                                    expandEdge.getEdgeTypeIds(),
                                                    expandEdge.getId(),
                                                    expandEdge.isBoth(),
                                                    newDetails);
                            boolean added = inputPattern.addEdge(src, dst, expandEdge);
                            if (!added) {
                                throw new UnsupportedOperationException(
                                        "edge "
                                                + expandEdge
                                                + " already exists in the pattern, and pattern with"
                                                + " multi-edges are not supported yet");
                            }
                            vertexOrEdgeDetails.put(
                                    expandEdge,
                                    new DataValue(pxd.getAliasName(), getFilters(expand)));
                            return expandEdge;
                        }

                        private PatternEdge visitEdge(
                                GraphLogicalExpand expand, PatternVertex src, PatternVertex dst) {
                            boolean isBoth = expand.getOpt() == GraphOpt.Expand.BOTH;
                            List<EdgeTypeId> edgeTypeIds =
                                    com.alibaba.graphscope.common.ir.meta.glogue.Utils
                                            .getEdgeTypeIds(expand);
                            int edgeId = idGenerator.getAndIncrement();
                            double selectivity = mq.getSelectivity(expand, getFilters(expand));
                            PatternEdge edge =
                                    (edgeTypeIds.size() == 1)
                                            ? new SinglePatternEdge(
                                                    src,
                                                    dst,
                                                    edgeTypeIds.get(0),
                                                    edgeId,
                                                    isBoth,
                                                    new ElementDetails(selectivity, optional))
                                            : new FuzzyPatternEdge(
                                                    src,
                                                    dst,
                                                    edgeTypeIds,
                                                    edgeId,
                                                    isBoth,
                                                    new ElementDetails(selectivity, optional));
                            return edge;
                        }
                    };
            for (RelNode sentence : sentences) {
                visitor.go(sentence);
            }
            return inputPattern;
        }

        private @Nullable RexNode getFilters(AbstractBindableTableScan tableScan) {
            List<RexNode> filters = Lists.newArrayList();
            if (tableScan instanceof GraphLogicalSource) {
                RexNode uniqueFilters = ((GraphLogicalSource) tableScan).getUniqueKeyFilters();
                if (uniqueFilters != null) {
                    filters.add(uniqueFilters);
                }
            }
            if (ObjectUtils.isNotEmpty(tableScan.getFilters())) {
                filters.addAll(tableScan.getFilters());
            }
            return filters.isEmpty()
                    ? null
                    : RexUtil.composeConjunction(builder.getRexBuilder(), filters);
        }

        private void checkPattern(Pattern pattern) {
            for (PatternEdge edge : pattern.getEdgeSet()) {
                PatternVertex src = edge.getSrcVertex();
                PatternVertex dst = edge.getDstVertex();
                Set<Integer> expectedSrcIds = Sets.newHashSet();
                Set<Integer> expectedDstIds = Sets.newHashSet();
                edge.getEdgeTypeIds()
                        .forEach(
                                k -> {
                                    if (!edge.isBoth()) {
                                        expectedSrcIds.add(k.getSrcLabelId());
                                        expectedDstIds.add(k.getDstLabelId());
                                    } else {
                                        expectedSrcIds.add(k.getSrcLabelId());
                                        expectedDstIds.add(k.getDstLabelId());
                                        expectedSrcIds.add(k.getDstLabelId());
                                        expectedDstIds.add(k.getSrcLabelId());
                                    }
                                });
                if (edge.getElementDetails().getRange() == null) {
                    Preconditions.checkArgument(
                            !edge.isBoth()
                                    ? Sets.newHashSet(src.getVertexTypeIds()).equals(expectedSrcIds)
                                    : expectedSrcIds.containsAll(src.getVertexTypeIds()),
                            "src vertex types %s not consistent with edge types %s",
                            src.getVertexTypeIds(),
                            edge.getEdgeTypeIds());
                    Preconditions.checkArgument(
                            !edge.isBoth()
                                    ? Sets.newHashSet(dst.getVertexTypeIds()).equals(expectedDstIds)
                                    : expectedDstIds.containsAll(dst.getVertexTypeIds()),
                            "dst vertex types %s not consistent with edge types %s",
                            dst.getVertexTypeIds(),
                            edge.getEdgeTypeIds());
                }
            }
        }
    }

    private class OutputConvertor extends GraphShuttle {
        private Map<DataKey, DataValue> details = Maps.newHashMap(graphDetails);

        @Override
        public RelNode visit(GraphPattern graph) {
            Pattern pattern = graph.getPattern();
            Preconditions.checkArgument(
                    pattern != null && pattern.getVertexNumber() == 1,
                    "can not convert pattern %s to any logical operator",
                    pattern);
            PatternVertex vertex = pattern.getVertexSet().iterator().next();
            VertexDataKey key = new VertexDataKey(pattern.getVertexOrder(vertex));
            DataValue value = getVertexValue(key, details, vertex);
            builder.source(
                    new SourceConfig(
                            GraphOpt.Source.VERTEX,
                            createLabels(vertex.getVertexTypeIds(), true),
                            value.getAlias()));
            if (value.getFilter() != null) {
                builder.filter(value.getFilter());
            }
            RelNode rel = builder.build();
            addCachedCost(rel, estimateCost(graph, vertex));
            return rel;
        }

        // estimate the detailed cost for a `Source` operator
        private DetailedSourceCost estimateCost(GraphPattern graph, PatternVertex singleVertex) {
            double selectivity = singleVertex.getElementDetails().getSelectivity();
            if (Double.compare(selectivity, 1.0d) < 0) {
                PatternVertex newVertex =
                        (singleVertex instanceof SinglePatternVertex)
                                ? new SinglePatternVertex(
                                        singleVertex.getVertexTypeIds().get(0),
                                        singleVertex.getId())
                                : new FuzzyPatternVertex(
                                        singleVertex.getVertexTypeIds(), singleVertex.getId());
                graph =
                        new GraphPattern(
                                graph.getCluster(), graph.getTraitSet(), new Pattern(newVertex));
            }
            double patternCount = mq.getRowCount(graph);
            return new DetailedSourceCost(patternCount, patternCount * selectivity);
        }

        // attach the computed cost to each operator to avoid redundant computation
        private RelNode addCachedCost(RelNode rel, RelOptCost cost) {
            if (rel instanceof AbstractBindableTableScan) {
                ((AbstractBindableTableScan) rel).setCachedCost(cost);
            } else if (rel instanceof GraphLogicalPathExpand) {
                ((GraphLogicalPathExpand) rel).setCachedCost(cost);
            } else if (rel instanceof LogicalJoin) {
                LogicalJoin join = (LogicalJoin) rel;
                return new LogicalJoin(
                        ((GraphOptCluster) join.getCluster())
                                .copy(new LocalState().withCachedCost(cost)),
                        join.getTraitSet(),
                        join.getHints(),
                        join.getLeft(),
                        join.getRight(),
                        join.getCondition(),
                        join.getVariablesSet(),
                        join.getJoinType(),
                        join.isSemiJoinDone(),
                        ImmutableList.copyOf(join.getSystemFieldList()));

            } else if (rel instanceof MultiJoin) {
                MultiJoin join = (MultiJoin) rel;
                return new MultiJoin(
                        ((GraphOptCluster) join.getCluster())
                                .copy(new LocalState().withCachedCost(cost)),
                        join.getInputs(),
                        join.getJoinFilter(),
                        join.getRowType(),
                        false,
                        join.getOuterJoinConditions(),
                        join.getJoinTypes(),
                        join.getProjFields(),
                        join.getJoinFieldRefCountsMap(),
                        null);
            }
            if (rel instanceof GraphLogicalGetV) {
                addCachedCost(rel.getInput(0), cost);
            }
            return rel;
        }

        // estimate the detailed cost for an `ExtendIntersect` operator
        private RelOptCost estimateCost(Pattern original, GraphExtendIntersect intersect, int idx) {
            GlogueExtendIntersectEdge edge = intersect.getGlogueEdge();
            ExtendEdge extendEdge = edge.getExtendStep().getExtendEdges().get(idx);
            PatternVertex start =
                    edge.getSrcPattern().getVertexByOrder(extendEdge.getSrcVertexOrder());
            PatternVertex target =
                    edge.getDstPattern()
                            .getVertexByOrder(edge.getExtendStep().getTargetVertexOrder());
            PatternEdge patternEdge =
                    com.alibaba.graphscope.common.ir.meta.glogue.Utils.convert(
                            extendEdge, start, target);
            EdgeCostEstimator<DetailedExpandCost> estimator =
                    new EdgeCostEstimator.Extend(
                            new CountHandler() {
                                @Override
                                public double handle(Pattern pattern) {
                                    return mq.getRowCount(
                                            new GraphPattern(
                                                    intersect.getCluster(),
                                                    intersect.getTraitSet(),
                                                    pattern));
                                }

                                @Override
                                public double labelConstraintsDeltaCost(
                                        PatternEdge edge, PatternVertex target) {
                                    return ((GraphRelMetadataQuery) mq)
                                            .getGlogueQuery()
                                            .getLabelConstraintsDeltaCost(edge, target);
                                }
                            });
            DetailedExpandCost cost = estimator.estimate(original, patternEdge, target);
            PatternVertex src = patternEdge.getSrcVertex();
            PatternVertex dst = patternEdge.getDstVertex();
            if (!original.containsVertex(src)) {
                original.addVertex(src);
            }
            if (!original.containsVertex(dst)) {
                original.addVertex(dst);
            }
            original.addEdge(src, dst, patternEdge);
            original.reordering();
            return cost;
        }

        @Override
        public RelNode visit(GraphExtendIntersect intersect) {
            GlogueExtendIntersectEdge glogueEdge = intersect.getGlogueEdge();
            Map<DataKey, DataValue> edgeDetails = getGlogueEdgeDetails(glogueEdge);
            this.details =
                    createSubDetails(
                            glogueEdge.getSrcPattern(),
                            glogueEdge.getSrcToTargetOrderMapping(),
                            null,
                            null);
            ExtendStep extendStep = glogueEdge.getExtendStep();
            List<ExtendEdge> extendEdges = extendStep.getExtendEdges();
            RelNode child = visitChildren(intersect).getInput(0);
            Pattern original = new Pattern(intersect.getGlogueEdge().getSrcPattern());
            original.reordering();
            // convert to GraphLogicalExpand if only one extend edge
            if (extendEdges.size() == 1) {
                return addCachedCost(
                        createExpandGetV(extendEdges.get(0), glogueEdge, edgeDetails, child),
                        estimateCost(original, intersect, 0));
            }
            // convert to Multi-Way join
            RelOptTable commonTable = new CommonOptTable(child);
            CommonTableScan commonScan =
                    new CommonTableScan(
                            intersect.getCluster(), intersect.getTraitSet(), commonTable);
            AtomicInteger counter = new AtomicInteger(0);
            List<RelNode> inputs =
                    extendEdges.stream()
                            .map(
                                    k -> {
                                        RelNode input =
                                                builder.push(
                                                                createExpandGetV(
                                                                        k,
                                                                        glogueEdge,
                                                                        edgeDetails,
                                                                        commonScan))
                                                        .build();
                                        RelOptCost estimatedCost =
                                                estimateCost(
                                                        original,
                                                        intersect,
                                                        counter.getAndIncrement());
                                        addCachedCost(input, estimatedCost);
                                        return input;
                                    })
                            .collect(Collectors.toList());
            RelNode rel =
                    new MultiJoin(
                            intersect.getCluster(),
                            inputs,
                            createIntersectFilter(glogueEdge, edgeDetails, inputs),
                            deriveIntersectType(inputs),
                            false,
                            Stream.generate(() -> (RexNode) null)
                                    .limit(inputs.size())
                                    .collect(Collectors.toList()),
                            Stream.generate(() -> JoinRelType.INNER)
                                    .limit(inputs.size())
                                    .collect(Collectors.toList()),
                            Stream.generate(() -> (ImmutableBitSet) null)
                                    .limit(inputs.size())
                                    .collect(Collectors.toList()),
                            ImmutableMap.of(),
                            null);
            return addCachedCost(
                    rel,
                    new RelOptCostImpl(
                            mq.getRowCount(
                                    new GraphPattern(
                                            intersect.getCluster(),
                                            intersect.getTraitSet(),
                                            glogueEdge.getDstPattern()))));
        }

        @Override
        public RelNode visit(GraphJoinDecomposition decomposition) {
            Map<Integer, Integer> probeOrderMap =
                    decomposition.getOrderMappings().getLeftToTargetOrderMap();
            Map<Integer, Integer> buildOrderMap =
                    decomposition.getOrderMappings().getRightToTargetOrderMap();
            List<GraphJoinDecomposition.JoinVertexPair> jointVertices =
                    decomposition.getJoinVertexPairs();
            DataValue defaultValue = new DataValue(generatePxdSplitAlias(), null, null);
            Map<@Nullable DataKey, DataValue> parentVertexDetails =
                    getJointVertexDetails(
                            jointVertices, probeOrderMap, buildOrderMap, defaultValue);
            Map<DataKey, DataValue> probeDetails =
                    createSubDetails(
                            decomposition.getProbePattern(),
                            probeOrderMap,
                            new ParentPattern(decomposition.getParentPatten(), 0),
                            defaultValue);
            Map<DataKey, DataValue> buildDetails =
                    createSubDetails(
                            decomposition.getBuildPattern(),
                            buildOrderMap,
                            new ParentPattern(decomposition.getParentPatten(), 1),
                            defaultValue);
            RelNode joinLeft = decomposition.getLeft();
            RelNode joinRight = decomposition.getRight();
            this.details = probeDetails;
            RelNode newLeft = visitChild(decomposition, 0, joinLeft).getInput(0);
            this.details = buildDetails;
            RelNode newRight = visitChild(decomposition, 1, joinRight).getInput(1);
            RexNode joinCondition =
                    createJoinFilter(
                            jointVertices,
                            parentVertexDetails,
                            newLeft,
                            newRight,
                            probeOrderMap,
                            buildOrderMap,
                            decomposition.getProbePattern(),
                            decomposition.getBuildPattern());
            // here we assume all inputs of the join come from different sources
            builder.push(newLeft).push(newRight).join(JoinRelType.INNER, joinCondition);
            // Handling special cases in decomposition:
            // When a join splitting is based on path expand, to ensure the consistency of the data
            // model with the original semantics,
            // we perform a concat operation on the left path and right path after the path join,
            // that is, merging them back into the original, un-split path.
            List<RexNode> concatExprs = Lists.newArrayList();
            List<String> concatAliases = Lists.newArrayList();
            jointVertices.forEach(
                    joint -> {
                        PatternVertex probeJointVertex =
                                decomposition
                                        .getProbePattern()
                                        .getVertexByOrder(joint.getLeftOrderId());
                        PatternVertex buildJointVertex =
                                decomposition
                                        .getBuildPattern()
                                        .getVertexByOrder(joint.getRightOrderId());
                        Set<PatternEdge> probeEdges =
                                decomposition.getProbePattern().getEdgesOf(probeJointVertex);
                        Set<PatternEdge> buildEdges =
                                decomposition.getBuildPattern().getEdgesOf(buildJointVertex);
                        if (probeEdges.size() == 1 && buildEdges.size() == 1) {
                            PatternEdge probeEdge = probeEdges.iterator().next();
                            DataValue probeValue =
                                    probeDetails.get(
                                            new EdgeDataKey(
                                                    decomposition
                                                            .getProbePattern()
                                                            .getVertexOrder(
                                                                    probeEdge.getSrcVertex()),
                                                    decomposition
                                                            .getProbePattern()
                                                            .getVertexOrder(
                                                                    probeEdge.getDstVertex()),
                                                    probeEdge.isBoth()
                                                            ? PatternDirection.BOTH
                                                            : PatternDirection.OUT));
                            PatternEdge buildEdge = buildEdges.iterator().next();
                            DataValue buildValue =
                                    buildDetails.get(
                                            new EdgeDataKey(
                                                    decomposition
                                                            .getBuildPattern()
                                                            .getVertexOrder(
                                                                    buildEdge.getSrcVertex()),
                                                    decomposition
                                                            .getBuildPattern()
                                                            .getVertexOrder(
                                                                    buildEdge.getDstVertex()),
                                                    buildEdge.isBoth()
                                                            ? PatternDirection.BOTH
                                                            : PatternDirection.OUT));
                            if (probeValue != null
                                    && probeValue.getParentAlias() != null
                                    && buildValue != null
                                    && buildValue.getParentAlias() != null
                                    && probeValue
                                            .getParentAlias()
                                            .equals(buildValue.getParentAlias())) {
                                String probeAlias = probeValue.getAlias();
                                String buildAlias = buildValue.getAlias();
                                concatExprs.add(
                                        builder.call(
                                                GraphStdOperatorTable.PATH_CONCAT,
                                                builder.variable(probeAlias),
                                                builder.getRexBuilder()
                                                        .makeFlag(
                                                                getConcatDirection(
                                                                        probeJointVertex,
                                                                        joinLeft)),
                                                builder.variable(buildAlias),
                                                builder.getRexBuilder()
                                                        .makeFlag(
                                                                getConcatDirection(
                                                                        buildJointVertex,
                                                                        joinRight))));
                                concatAliases.add(probeValue.getParentAlias());
                            }
                        }
                    });
            if (!concatExprs.isEmpty()) {
                // TODO(yihe.zxl): there are additional optimization opportunities here by employing
                // projects with append=false, the left path and right path prior to merging can be
                // removed.
                builder.project(concatExprs, concatAliases, true);
            }
            RelNode rel = builder.build();
            RelOptCost cost =
                    new RelOptCostImpl(
                            mq.getRowCount(
                                    new GraphPattern(
                                            decomposition.getCluster(),
                                            decomposition.getTraitSet(),
                                            decomposition.getParentPatten())));
            return addCachedCost(rel, cost);
        }

        private String generatePxdSplitAlias() {
            return "PATTERN_SPLIT_VERTEX$" + UUID.randomUUID().hashCode();
        }

        private GraphOpt.GetV getConcatDirection(PatternVertex concatVertex, RelNode splitPattern) {
            ConcatDirectionVisitor visitor = new ConcatDirectionVisitor(concatVertex);
            visitor.go(splitPattern);
            return visitor.direction;
        }

        private RexNode createJoinFilter(
                List<GraphJoinDecomposition.JoinVertexPair> jointVertices,
                Map<DataKey, DataValue> vertexDetails,
                RelNode left,
                RelNode right,
                Map<Integer, Integer> probeOrderMap,
                Map<Integer, Integer> buildOrderMap,
                Pattern probePattern,
                Pattern buildPattern) {
            List<RexNode> joinCondition = Lists.newArrayList();
            List<RelDataTypeField> leftFields =
                    com.alibaba.graphscope.common.ir.tools.Utils.getOutputType(left).getFieldList();
            for (GraphJoinDecomposition.JoinVertexPair jointVertex : jointVertices) {
                builder.push(left);
                RexNode leftVar =
                        convert(jointVertex.left, vertexDetails, probeOrderMap, probePattern);
                builder.build();
                builder.push(right);
                RexGraphVariable rightVar =
                        convert(jointVertex.right, vertexDetails, buildOrderMap, buildPattern);
                rightVar =
                        (rightVar.getProperty() == null)
                                ? RexGraphVariable.of(
                                        rightVar.getAliasId(),
                                        leftFields.size() + rightVar.getIndex(),
                                        rightVar.getName(),
                                        rightVar.getType())
                                : RexGraphVariable.of(
                                        rightVar.getAliasId(),
                                        rightVar.getProperty(),
                                        leftFields.size() + rightVar.getIndex(),
                                        rightVar.getName(),
                                        rightVar.getType());
                builder.build();
                joinCondition.add(builder.equals(leftVar, rightVar));
            }
            return RexUtil.composeConjunction(builder.getRexBuilder(), joinCondition);
        }

        private RexGraphVariable convert(
                JoinVertexEntry<Integer> joinVertex,
                Map<@Nullable DataKey, DataValue> vertexDetails,
                Map<Integer, Integer> orderMap,
                Pattern pattern) {
            Integer orderId = orderMap.get(joinVertex.getVertex());
            VertexDataKey vertexKey = (orderId == null) ? null : new VertexDataKey(orderId);
            DataValue value =
                    getVertexValue(
                            vertexKey,
                            vertexDetails,
                            pattern.getVertexByOrder(joinVertex.getVertex()));
            return joinVertex.getKeyName() != null
                    ? builder.variable(value.getAlias(), joinVertex.getKeyName())
                    : builder.variable(value.getAlias());
        }

        private Map<@Nullable DataKey, DataValue> getJointVertexDetails(
                List<GraphJoinDecomposition.JoinVertexPair> jointVertices,
                Map<Integer, Integer> probeOrderMap,
                Map<Integer, Integer> buildOrderMap,
                DataValue defaultValue) {
            Map<DataKey, DataValue> vertexDetails = Maps.newHashMap();
            jointVertices.forEach(
                    k -> {
                        Integer buildOrderId = buildOrderMap.get(k.getRightOrderId());
                        if (buildOrderId != null) {
                            VertexDataKey dataKey = new VertexDataKey(buildOrderId);
                            DataValue value = details.get(dataKey);
                            if (value != null) {
                                vertexDetails.put(dataKey, value);
                            }
                        }
                        Integer probeOrderId = probeOrderMap.get(k.getLeftOrderId());
                        if (probeOrderId != null) {
                            VertexDataKey dataKey = new VertexDataKey(probeOrderId);
                            DataValue value = details.get(dataKey);
                            if (!vertexDetails.containsKey(dataKey) && value != null) {
                                vertexDetails.put(dataKey, value);
                            }
                        }
                        if (buildOrderId == null && probeOrderId == null) {
                            vertexDetails.put(null, defaultValue);
                        }
                    });
            return vertexDetails;
        }

        private @Nullable RelDataType deriveIntersectType(List<RelNode> inputs) {
            return inputs.isEmpty() ? null : Utils.getOutputType(inputs.get(0));
        }

        private RexNode createIntersectFilter(
                GlogueExtendIntersectEdge glogueEdge,
                Map<DataKey, DataValue> edgeDetails,
                List<RelNode> inputs) {
            ExtendStep step = glogueEdge.getExtendStep();
            VertexDataKey targetKey = new VertexDataKey(step.getTargetVertexOrder());
            DataValue targetValue =
                    getVertexValue(
                            targetKey,
                            edgeDetails,
                            glogueEdge
                                    .getDstPattern()
                                    .getVertexByOrder(step.getTargetVertexOrder()));
            String alias = targetValue.getAlias();
            List<RexNode> intersectFilters = Lists.newArrayList();
            for (int i = 0; i < inputs.size() - 1; ++i) {
                builder.push(inputs.get(i));
                RexVariable left = builder.variable(alias);
                builder.push(inputs.get(i + 1));
                RexVariable right = builder.variable(alias);
                intersectFilters.add(builder.equals(left, right));
            }
            return RexUtil.composeConjunction(builder.getRexBuilder(), intersectFilters);
        }

        private RelNode createExpandGetV(
                ExtendEdge edge,
                GlogueExtendIntersectEdge glogueEdge,
                Map<DataKey, DataValue> edgeDetails,
                RelNode input) {
            builder.push(input);
            ExtendStep extendStep = glogueEdge.getExtendStep();
            DataValue edgeValue = getEdgeValue(createEdgeKey(edge, glogueEdge), edgeDetails);
            Map<Integer, Integer> srcToTargetMap = glogueEdge.getSrcToTargetOrderMapping();
            Integer srcInTargetOrderId = srcToTargetMap.get(edge.getSrcVertexOrder());
            if (srcInTargetOrderId == null) {
                srcInTargetOrderId = -1;
            }
            PatternVertex src =
                    glogueEdge.getSrcPattern().getVertexByOrder(edge.getSrcVertexOrder());
            DataValue srcValue =
                    getVertexValue(new VertexDataKey(srcInTargetOrderId), edgeDetails, src);
            PatternVertex target =
                    glogueEdge.getDstPattern().getVertexByOrder(extendStep.getTargetVertexOrder());
            DataValue targetValue =
                    getVertexValue(
                            new VertexDataKey(extendStep.getTargetVertexOrder()),
                            edgeDetails,
                            target);
            ExpandConfig expandConfig =
                    new ExpandConfig(
                            createExpandOpt(edge.getDirection()),
                            createLabels(
                                    edge.getEdgeTypeIds().stream()
                                            .map(k -> k.getEdgeLabelId())
                                            .collect(Collectors.toList()),
                                    false),
                            edgeValue.getAlias(),
                            srcValue.getAlias());
            GraphLabelType tripletEdgeType = createTripletEdgeType(edge.getEdgeTypeIds());
            GetVConfig getVConfig =
                    new GetVConfig(
                            createGetVOpt(edge.getDirection()),
                            createLabels(target.getVertexTypeIds(), true),
                            targetValue.getAlias());
            if (edge.getElementDetails().getRange() != null) {
                PathExpandConfig.Builder pxdBuilder = PathExpandConfig.newBuilder(builder);
                pxdBuilder.expand(expandConfig);
                if (edgeValue.getFilter() != null) {
                    pxdBuilder.filter(edgeValue.getFilter());
                }
                GetVConfig innerGetVConfig =
                        new GetVConfig(
                                getVConfig.getOpt(),
                                createLabels(
                                        createInnerGetVTypes(
                                                src.getVertexTypeIds(),
                                                target.getVertexTypeIds(),
                                                edge.getElementDetails()),
                                        true),
                                getVConfig.getAlias());
                GraphOpt.PathExpandResult resultOpt = edge.getElementDetails().getResultOpt();
                GraphOpt.PathExpandPath pathOpt = edge.getElementDetails().getPathOpt();
                pxdBuilder
                        .getV(innerGetVConfig)
                        .resultOpt(resultOpt == null ? GraphOpt.PathExpandResult.END_V : resultOpt)
                        .pathOpt(pathOpt == null ? GraphOpt.PathExpandPath.ARBITRARY : pathOpt)
                        .alias(edgeValue.getAlias())
                        .startAlias(srcValue.getAlias())
                        .range(
                                edge.getElementDetails().getRange().getOffset(),
                                edge.getElementDetails().getRange().getFetch());
                GraphLogicalPathExpand pxd =
                        (GraphLogicalPathExpand)
                                builder.pathExpand(pxdBuilder.buildConfig()).build();
                GraphLogicalExpand expand = (GraphLogicalExpand) pxd.getExpand();
                GraphSchemaType edgeType =
                        (GraphSchemaType) expand.getRowType().getFieldList().get(0).getType();
                expand.setSchemaType(createSchemaType(tripletEdgeType, edgeType));
                builder.push(
                                createPathExpandWithOptional(
                                        pxd, edge.getElementDetails().isOptional()))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        getVConfig.getLabels(),
                                        getVConfig.getAlias(),
                                        getVConfig.getStartAlias()));
            } else {
                GraphLogicalExpand expand =
                        createExpandWithOptional(
                                (GraphLogicalExpand) builder.expand(expandConfig).build(),
                                edge.getElementDetails().isOptional());
                GraphSchemaType edgeType =
                        (GraphSchemaType) expand.getRowType().getFieldList().get(0).getType();
                expand.setSchemaType(createSchemaType(tripletEdgeType, edgeType));
                builder.push(expand);
                if (edgeValue.getFilter() != null) {
                    builder.filter(edgeValue.getFilter());
                }
                builder.getV(getVConfig);
            }
            if (targetValue.getFilter() != null) {
                builder.filter(targetValue.getFilter());
            }
            return builder.build();
        }

        // Handle a special case for path expand: re-infer the type of inner getV in path expand.
        // Before optimization, the type of path expand is inferred based on the user's query order,
        // resulting in the type of inner endV being bound to this order.
        // However, if the optimizer reverses the order of the path expand, the type of inner endV
        // also needs to be reversed accordingly.
        private List<Integer> createInnerGetVTypes(
                List<Integer> startVTypes, List<Integer> endVTypes, ElementDetails details) {
            List<Integer> originalInnerVTypes = details.getPxdInnerGetVTypes();
            if (!originalInnerVTypes.isEmpty()) {
                if (!endVTypes.isEmpty() && originalInnerVTypes.containsAll(endVTypes)) {
                    return originalInnerVTypes;
                } else if (!startVTypes.isEmpty() && originalInnerVTypes.containsAll(startVTypes)) {
                    List<Integer> reverseTypes = Lists.newArrayList(originalInnerVTypes);
                    reverseTypes.removeAll(startVTypes);
                    reverseTypes.addAll(endVTypes);
                    return reverseTypes.stream().distinct().collect(Collectors.toList());
                }
            }
            return endVTypes;
        }

        private GraphLogicalPathExpand createPathExpandWithOptional(
                GraphLogicalPathExpand pxd, boolean optional) {
            if (pxd.getFused() != null) {
                return GraphLogicalPathExpand.create(
                        (GraphOptCluster) pxd.getCluster(),
                        ImmutableList.of(),
                        pxd.getInput(),
                        pxd.getFused(),
                        pxd.getOffset(),
                        pxd.getFetch(),
                        pxd.getResultOpt(),
                        pxd.getPathOpt(),
                        pxd.getUntilCondition(),
                        pxd.getAliasName(),
                        pxd.getStartAlias(),
                        optional);
            } else {
                return GraphLogicalPathExpand.create(
                        (GraphOptCluster) pxd.getCluster(),
                        ImmutableList.of(),
                        pxd.getInput(),
                        pxd.getExpand(),
                        pxd.getGetV(),
                        pxd.getOffset(),
                        pxd.getFetch(),
                        pxd.getResultOpt(),
                        pxd.getPathOpt(),
                        pxd.getUntilCondition(),
                        pxd.getAliasName(),
                        pxd.getStartAlias(),
                        optional);
            }
        }

        private GraphLogicalExpand createExpandWithOptional(
                GraphLogicalExpand expand, boolean optional) {
            return GraphLogicalExpand.create(
                    (GraphOptCluster) expand.getCluster(),
                    expand.getHints(),
                    expand.getInput(0),
                    expand.getOpt(),
                    expand.getTableConfig(),
                    expand.getAliasName(),
                    expand.getStartAlias(),
                    optional);
        }

        private DataValue getVertexValue(
                VertexDataKey key, Map<DataKey, DataValue> edgeDetails, PatternVertex vertex) {
            DataValue vertexValue = edgeDetails.get(key);
            return (vertexValue != null)
                    ? vertexValue
                    : new DataValue("PATTERN_VERTEX$" + vertex.getId(), null);
        }

        private DataValue getEdgeValue(EdgeDataKey key, Map<DataKey, DataValue> edgeDetails) {
            DataValue edgeValue = edgeDetails.get(key);
            return (edgeValue != null)
                    ? edgeValue
                    : new DataValue(AliasInference.DEFAULT_NAME, null);
        }

        private GraphLabelType createTripletEdgeType(List<EdgeTypeId> edgeTypeIds) {
            List<GraphLabelType.Entry> entries = Lists.newArrayList();
            IrGraphSchema schema = irMeta.getSchema();
            for (EdgeTypeId typeId : edgeTypeIds) {
                GraphEdge edgeWithTypeId = null;
                for (GraphEdge edge : schema.getEdgeList()) {
                    if (edge.getLabelId() == typeId.getEdgeLabelId()) {
                        edgeWithTypeId = edge;
                        break;
                    }
                }
                if (edgeWithTypeId != null) {
                    for (EdgeRelation relation : edgeWithTypeId.getRelationList()) {
                        GraphVertex src = relation.getSource();
                        GraphVertex dst = relation.getTarget();
                        if (src.getLabelId() == typeId.getSrcLabelId()
                                && dst.getLabelId() == typeId.getDstLabelId()) {
                            entries.add(
                                    new GraphLabelType.Entry()
                                            .label(edgeWithTypeId.getLabel())
                                            .labelId(edgeWithTypeId.getLabelId())
                                            .srcLabel(src.getLabel())
                                            .srcLabelId(src.getLabelId())
                                            .dstLabel(dst.getLabel())
                                            .dstLabelId(dst.getLabelId()));
                            break;
                        }
                    }
                }
            }
            return new GraphLabelType(entries);
        }

        private GraphSchemaType createSchemaType(
                GraphLabelType labelType, GraphSchemaType originalType) {
            if (labelType.getLabelsEntry().size() == 1) {
                return new GraphSchemaType(
                        originalType.getScanOpt(),
                        labelType,
                        originalType.getFieldList(),
                        originalType.isNullable());
            } else {
                List<GraphSchemaType> fuzzyTypes =
                        labelType.getLabelsEntry().stream()
                                .map(
                                        k ->
                                                new GraphSchemaType(
                                                        originalType.getScanOpt(),
                                                        new GraphLabelType(k),
                                                        originalType.getFieldList(),
                                                        originalType.isNullable()))
                                .collect(Collectors.toList());
                return GraphSchemaType.create(
                        fuzzyTypes, builder.getTypeFactory(), originalType.isNullable());
            }
        }

        private LabelConfig createLabels(List<Integer> typeIds, boolean isVertex) {
            IrGraphSchema schema = irMeta.getSchema();
            List<String> labels =
                    isVertex
                            ? schema.getVertexList().stream()
                                    .filter(v -> typeIds.contains(v.getLabelId()))
                                    .map(k -> k.getLabel())
                                    .collect(Collectors.toList())
                            : schema.getEdgeList().stream()
                                    .filter(v -> typeIds.contains(v.getLabelId()))
                                    .map(k -> k.getLabel())
                                    .collect(Collectors.toList());
            LabelConfig config = new LabelConfig(false);
            labels.forEach(
                    k -> {
                        config.addLabel(k);
                    });
            return config;
        }

        private GraphOpt.Expand createExpandOpt(PatternDirection direction) {
            return GraphOpt.Expand.valueOf(direction.name());
        }

        private GraphOpt.GetV createGetVOpt(PatternDirection direction) {
            switch (direction) {
                case IN:
                    return GraphOpt.GetV.START;
                case OUT:
                    return GraphOpt.GetV.END;
                case BOTH:
                default:
                    return GraphOpt.GetV.OTHER;
            }
        }

        private Map<DataKey, DataValue> getGlogueEdgeDetails(GlogueExtendIntersectEdge edge) {
            Map<DataKey, DataValue> edgeDetails = Maps.newHashMap();
            Map<Integer, Integer> srcToTargetMap = edge.getSrcToTargetOrderMapping();
            ExtendStep extendStep = edge.getExtendStep();
            VertexDataKey targetKey = new VertexDataKey(extendStep.getTargetVertexOrder());
            DataValue targetValue = details.get(targetKey);
            if (targetValue != null) {
                edgeDetails.put(targetKey, targetValue);
            }
            extendStep
                    .getExtendEdges()
                    .forEach(
                            k -> {
                                EdgeDataKey key = createEdgeKey(k, edge);
                                DataValue value = details.get(key);
                                if (value != null) {
                                    edgeDetails.put(key, value);
                                }
                                Integer srcInTargetOrderId =
                                        srcToTargetMap.get(k.getSrcVertexOrder());
                                if (srcInTargetOrderId != null) {
                                    VertexDataKey key2 = new VertexDataKey(srcInTargetOrderId);
                                    DataValue value2 = details.get(key2);
                                    if (value2 != null) {
                                        edgeDetails.put(key2, value2);
                                    }
                                }
                            });
            return edgeDetails;
        }

        private class ParentPattern {
            private final Pattern pattern;
            private final int subId;

            public ParentPattern(Pattern pattern, int subId) {
                this.pattern = pattern;
                this.subId = subId;
            }
        }

        private Map<DataKey, DataValue> createSubDetails(
                Pattern subPattern,
                Map<Integer, Integer> orderMappings,
                @Nullable ParentPattern parentPattern,
                @Nullable DataValue defaultValue) {
            Map<DataKey, DataValue> newDetails = Maps.newHashMap();
            subPattern
                    .getVertexSet()
                    .forEach(
                            k -> {
                                int newOrderId = subPattern.getVertexOrder(k);
                                Integer oldOrderId = orderMappings.get(newOrderId);
                                if (oldOrderId != null) {
                                    DataValue value = details.get(new VertexDataKey(oldOrderId));
                                    if (value != null) {
                                        newDetails.put(new VertexDataKey(newOrderId), value);
                                    }
                                } else if (defaultValue != null) {
                                    newDetails.put(new VertexDataKey(newOrderId), defaultValue);
                                }
                            });
            subPattern
                    .getEdgeSet()
                    .forEach(
                            k -> {
                                int newSrcOrderId = subPattern.getVertexOrder(k.getSrcVertex());
                                int newDstOrderId = subPattern.getVertexOrder(k.getDstVertex());
                                Integer oldSrcOrderId = orderMappings.get(newSrcOrderId);
                                Integer oldDstOrderId = orderMappings.get(newDstOrderId);
                                PatternDirection direction =
                                        k.isBoth() ? PatternDirection.BOTH : PatternDirection.OUT;
                                EdgeDataKey oldKey = null;
                                boolean splitPathExpand = false;
                                if (oldSrcOrderId != null && oldDstOrderId != null) {
                                    oldKey =
                                            new EdgeDataKey(
                                                    oldSrcOrderId, oldDstOrderId, direction);
                                } else if (parentPattern != null) {
                                    // here we use a hack way to find the original edge key in the
                                    // parent pattern for the split path expand,
                                    // in <JoinDecompositionRule>, we guarantee the split edge id
                                    // consistent with the parent's, here we just use the split edge
                                    // id to find the original edge in the parent pattern.
                                    Pattern pattern = parentPattern.pattern;
                                    for (PatternEdge edge : pattern.getEdgeSet()) {
                                        if (k.getId() == edge.getId()) {
                                            oldKey =
                                                    new EdgeDataKey(
                                                            pattern.getVertexOrder(
                                                                    edge.getSrcVertex()),
                                                            pattern.getVertexOrder(
                                                                    edge.getDstVertex()),
                                                            direction);
                                            splitPathExpand = true;
                                            break;
                                        }
                                    }
                                }
                                if (oldKey != null) {
                                    DataValue value = details.get(oldKey);
                                    if (value != null) {
                                        EdgeDataKey newKey =
                                                new EdgeDataKey(
                                                        newSrcOrderId, newDstOrderId, direction);
                                        if (splitPathExpand
                                                && !AliasInference.isDefaultAlias(
                                                        value.getAlias())) {
                                            // assign a new alias tagged with subId for the split
                                            // path expand, i.e. if the original path expand is
                                            // '[a:KNOWS*6..7]',
                                            // after splitting, we get left split path expand
                                            // '[a$p_0:KNOWS*3..4]', and right split path expand
                                            // '[a$p_1:KNOWS*3..4]',
                                            value =
                                                    new DataValue(
                                                            value.getAlias()
                                                                    + "$p_"
                                                                    + parentPattern.subId,
                                                            value.getFilter(),
                                                            value.getAlias());
                                        }
                                        newDetails.put(newKey, value);
                                    }
                                }
                            });
            return newDetails;
        }

        private EdgeDataKey createEdgeKey(ExtendEdge edge, GlogueExtendIntersectEdge glogueEdge) {
            int targetOrderId = glogueEdge.getExtendStep().getTargetVertexOrder();
            Map<Integer, Integer> srcToTargetMap = glogueEdge.getSrcToTargetOrderMapping();
            Integer srcOrderId = srcToTargetMap.get(edge.getSrcVertexOrder());
            if (srcOrderId == null) {
                srcOrderId = -1;
            }
            return new EdgeDataKey(srcOrderId, targetOrderId, edge.getDirection());
        }

        // given a concat vertex, help to determine its direction in the split path expand
        private class ConcatDirectionVisitor extends RelVisitor {
            private GraphOpt.GetV direction;
            private final PatternVertex concatVertex;

            public ConcatDirectionVisitor(PatternVertex concatVertex) {
                this.concatVertex = concatVertex;
                this.direction = null;
            }

            @Override
            public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                if (direction != null) return;
                if (node instanceof GraphExtendIntersect) {
                    GlogueExtendIntersectEdge intersect =
                            ((GraphExtendIntersect) node).getGlogueEdge();
                    ExtendStep extendStep = intersect.getExtendStep();
                    PatternVertex dstVertex =
                            intersect
                                    .getDstPattern()
                                    .getVertexByOrder(extendStep.getTargetVertexOrder());
                    if (dstVertex.equals(concatVertex)) {
                        direction = GraphOpt.GetV.END;
                        return;
                    }
                    for (ExtendEdge edge : extendStep.getExtendEdges()) {
                        PatternVertex srcVertex =
                                intersect
                                        .getSrcPattern()
                                        .getVertexByOrder(edge.getSrcVertexOrder());
                        if (srcVertex.equals(concatVertex)) {
                            direction = GraphOpt.GetV.START;
                            return;
                        }
                    }
                } else if (node instanceof GraphPattern) {
                    PatternVertex vertex =
                            ((GraphPattern) node).getPattern().getVertexSet().iterator().next();
                    if (vertex.equals(concatVertex)) {
                        direction = GraphOpt.GetV.START;
                        return;
                    }
                }
                node.childrenAccept(this);
            }
        }
    }
}
