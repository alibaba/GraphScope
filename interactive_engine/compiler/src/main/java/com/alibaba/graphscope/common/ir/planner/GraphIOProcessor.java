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

import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.alibaba.graphscope.common.ir.planner.type.DataKey;
import com.alibaba.graphscope.common.ir.planner.type.DataValue;
import com.alibaba.graphscope.common.ir.planner.type.EdgeDataKey;
import com.alibaba.graphscope.common.ir.planner.type.VertexDataKey;
import com.alibaba.graphscope.common.ir.rel.CommonTableScan;
import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.GraphRelShuttleX;
import com.alibaba.graphscope.common.ir.rel.graph.AbstractBindableTableScan;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendStep;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueExtendIntersectEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.Utils;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.alibaba.graphscope.common.store.IrMeta;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
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
        if (!graphDetails.isEmpty()) {
            graphDetails.clear();
        }
        return input.accept(new InputConvertor());
    }

    /**
     * convert {@code Intersect} to {@code Expand} or {@code MultiJoin}
     * @param output
     * @return
     */
    public RelNode processOutput(RelNode output) {
        return output.accept(new OutputConvertor());
    }

    public Map<DataKey, DataValue> getGraphDetails() {
        return Collections.unmodifiableMap(this.graphDetails);
    }

    private class InputConvertor extends GraphRelShuttleX {
        @Override
        public RelNode visit(GraphLogicalMultiMatch match) {
            return new GraphPattern(
                    match.getCluster(), match.getTraitSet(), visit(match.getSentences()));
        }

        @Override
        public RelNode visit(GraphLogicalSingleMatch match) {
            return new GraphPattern(
                    match.getCluster(),
                    match.getTraitSet(),
                    visit(ImmutableList.of(match.getSentence())));
        }

        private Pattern visit(List<RelNode> sentences) {
            Pattern pattern = new Pattern();
            pattern.setPatternId(UUID.randomUUID().hashCode());
            Map<Object, DataValue> vertexOrEdgeDetails = Maps.newHashMap();
            RelVisitor visitor =
                    new RelVisitor() {
                        Map<String, PatternVertex> aliasNameToVertex = Maps.newHashMap();
                        AtomicInteger idGenerator = new AtomicInteger(0);
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
                                pattern.addVertex(existVertex);
                                if (alias != AliasInference.DEFAULT_NAME) {
                                    aliasNameToVertex.put(alias, existVertex);
                                }
                                vertexOrEdgeDetails.put(existVertex, new DataValue(alias, filters));
                            } else if (filters != null) {
                                DataValue value = vertexOrEdgeDetails.get(existVertex);
                                if (value.getFilter() == null
                                        || !RelOptUtil.conjunctions(value.getFilter())
                                                .contains(filters)) {
                                    throw new IllegalArgumentException(
                                            "filters "
                                                    + filters
                                                    + " not exist in the previous vertex filters "
                                                    + value.getFilter());
                                }
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
                                                    new ElementDetails(selectivity))
                                            : new FuzzyPatternEdge(
                                                    src,
                                                    dst,
                                                    edgeTypeIds,
                                                    edgeId,
                                                    isBoth,
                                                    new ElementDetails(selectivity));
                            pattern.addEdge(src, dst, edge);
                            vertexOrEdgeDetails.put(
                                    edge, new DataValue(expand.getAliasName(), getFilters(expand)));
                            return edge;
                        }
                    };
            for (RelNode sentence : sentences) {
                visitor.go(sentence);
            }
            pattern.reordering();
            vertexOrEdgeDetails.forEach(
                    (k, v) -> {
                        DataKey key = null;
                        if (k instanceof PatternVertex) {
                            key = new VertexDataKey(pattern.getVertexOrder((PatternVertex) k));
                        } else if (k instanceof PatternEdge) {
                            int srcOrderId =
                                    pattern.getVertexOrder(((PatternEdge) k).getSrcVertex());
                            int dstOrderId =
                                    pattern.getVertexOrder(((PatternEdge) k).getDstVertex());
                            PatternDirection direction =
                                    ((PatternEdge) k).isBoth()
                                            ? PatternDirection.BOTH
                                            : PatternDirection.OUT;
                            key = new EdgeDataKey(srcOrderId, dstOrderId, direction);
                        }
                        graphDetails.put(key, v);
                    });
            checkPattern(pattern);
            return pattern;
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
                                    expectedSrcIds.add(k.getSrcLabelId());
                                    expectedDstIds.add(k.getDstLabelId());
                                });
                Preconditions.checkArgument(
                        Sets.newHashSet(src.getVertexTypeIds()).equals(expectedSrcIds),
                        "src vertex types %s not consistent with edge types %s",
                        src.getVertexTypeIds(),
                        edge.getEdgeTypeIds());
                Preconditions.checkArgument(
                        Sets.newHashSet(dst.getVertexTypeIds()).equals(expectedDstIds),
                        "dst vertex types %s not consistent with edge types %s",
                        dst.getVertexTypeIds(),
                        edge.getEdgeTypeIds());
            }
        }
    }

    private class OutputConvertor extends GraphRelShuttleX {
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
            DataValue value;
            Preconditions.checkArgument(
                    (value = details.get(key)) != null,
                    "can not find vertex key %s in details map %s",
                    key,
                    details);
            builder.source(
                    new SourceConfig(
                            GraphOpt.Source.VERTEX,
                            createLabels(vertex.getVertexTypeIds(), true),
                            value.getAlias()));
            if (value.getFilter() != null) {
                builder.filter(value.getFilter());
            }
            return builder.build();
        }

        @Override
        public RelNode visit(GraphExtendIntersect intersect) {
            GlogueExtendIntersectEdge glogueEdge = intersect.getGlogueEdge();
            Map<DataKey, DataValue> edgeDetails = getGlogueEdgeDetails(glogueEdge);
            ExtendStep extendStep = glogueEdge.getExtendStep();
            List<ExtendEdge> extendEdges = extendStep.getExtendEdges();
            RelNode child = visitChildren(intersect).getInput(0);
            // convert to GraphLogicalExpand if only one extend edge
            if (extendEdges.size() == 1) {
                return createExpandGetV(extendEdges.get(0), glogueEdge, edgeDetails, child);
            }
            // convert to Multi-Way join
            RelOptTable commonTable = new CommonOptTable(child);
            CommonTableScan commonScan =
                    new CommonTableScan(
                            intersect.getCluster(), intersect.getTraitSet(), commonTable);
            List<RelNode> inputs =
                    extendEdges.stream()
                            .map(
                                    k ->
                                            builder.push(
                                                            createExpandGetV(
                                                                    k,
                                                                    glogueEdge,
                                                                    edgeDetails,
                                                                    commonScan))
                                                    .build())
                            .collect(Collectors.toList());
            return new MultiJoin(
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
            DataValue targetValue;
            Preconditions.checkArgument(
                    (targetValue = edgeDetails.get(targetKey)) != null,
                    "can not find target vertex key %s in details map %s",
                    targetKey,
                    edgeDetails);
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
            EdgeDataKey key = createEdgeKey(edge, glogueEdge);
            DataValue value = edgeDetails.get(key);
            Preconditions.checkArgument(
                    value != null, "can not find edge key %s in details map %s", key, edgeDetails);
            Map<Integer, Integer> srcToTargetMap = glogueEdge.getSrcToTargetOrderMapping();
            VertexDataKey srcKey = new VertexDataKey(srcToTargetMap.get(edge.getSrcVertexOrder()));
            DataValue srcValue;
            Preconditions.checkArgument(
                    (srcValue = edgeDetails.get(srcKey)) != null,
                    "can not find src vertex key %s in details map %s",
                    srcKey,
                    edgeDetails);
            builder.expand(
                    new ExpandConfig(
                            createExpandOpt(edge.getDirection()),
                            createLabels(
                                    edge.getEdgeTypeIds().stream()
                                            .map(k -> k.getEdgeLabelId())
                                            .collect(Collectors.toList()),
                                    false),
                            value.getAlias(),
                            srcValue.getAlias()));
            if (value.getFilter() != null) {
                builder.filter(value.getFilter());
            }
            int targetOrderId = extendStep.getTargetVertexOrder();
            PatternVertex target = glogueEdge.getDstPattern().getVertexByOrder(targetOrderId);
            VertexDataKey targetKey = new VertexDataKey(targetOrderId);
            DataValue targetValue = edgeDetails.get(targetKey);
            Preconditions.checkArgument(
                    targetValue != null,
                    "can not find target vertex key %s in details map %s",
                    targetKey,
                    edgeDetails);
            builder.getV(
                    new GetVConfig(
                            createGetVOpt(edge.getDirection()),
                            createLabels(target.getVertexTypeIds(), true),
                            targetValue.getAlias()));
            if (targetValue.getFilter() != null) {
                builder.filter(targetValue.getFilter());
            }
            return builder.build();
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
            Preconditions.checkArgument(
                    targetKey != null,
                    "can not find target vertex key %s in details map %s",
                    targetKey,
                    details);
            edgeDetails.put(targetKey, targetValue);
            extendStep
                    .getExtendEdges()
                    .forEach(
                            k -> {
                                EdgeDataKey key = createEdgeKey(k, edge);
                                DataValue value = details.get(key);
                                Preconditions.checkArgument(
                                        value != null,
                                        "can not find edge key %s in details map %s",
                                        key,
                                        details);
                                edgeDetails.put(key, value);
                                VertexDataKey key2 =
                                        new VertexDataKey(
                                                srcToTargetMap.get(k.getSrcVertexOrder()));
                                DataValue value2;
                                Preconditions.checkArgument(
                                        (value2 = details.get(key2)) != null,
                                        "can not find vertex key %s in details map %s",
                                        key2,
                                        details);
                                edgeDetails.put(key2, value2);
                            });
            // update details for the recursive invocation
            Pattern src = edge.getSrcPattern();
            Map<DataKey, DataValue> newDetails = Maps.newHashMap();
            src.getVertexSet()
                    .forEach(
                            k -> {
                                int newOrderId = src.getVertexOrder(k);
                                VertexDataKey oldKey =
                                        new VertexDataKey(srcToTargetMap.get(newOrderId));
                                VertexDataKey newKey = new VertexDataKey(newOrderId);
                                DataValue value = details.get(oldKey);
                                Preconditions.checkArgument(
                                        value != null,
                                        "can not find vertex key %s in details map %s",
                                        oldKey,
                                        details);
                                newDetails.put(newKey, value);
                            });
            src.getEdgeSet()
                    .forEach(
                            k -> {
                                int newSrcOrderId = src.getVertexOrder(k.getSrcVertex());
                                int newDstOrderId = src.getVertexOrder(k.getDstVertex());
                                PatternDirection direction =
                                        k.isBoth() ? PatternDirection.BOTH : PatternDirection.OUT;
                                EdgeDataKey oldKey =
                                        new EdgeDataKey(
                                                srcToTargetMap.get(newSrcOrderId),
                                                srcToTargetMap.get(newDstOrderId),
                                                direction);
                                EdgeDataKey newKey =
                                        new EdgeDataKey(newSrcOrderId, newDstOrderId, direction);
                                DataValue value = details.get(oldKey);
                                Preconditions.checkArgument(
                                        value != null,
                                        "can not find edge key %s in details map %s",
                                        oldKey,
                                        details);
                                newDetails.put(newKey, value);
                            });
            details = newDetails;
            return edgeDetails;
        }

        private EdgeDataKey createEdgeKey(ExtendEdge edge, GlogueExtendIntersectEdge glogueEdge) {
            int targetOrderId = glogueEdge.getExtendStep().getTargetVertexOrder();
            Map<Integer, Integer> srcToTargetMap = glogueEdge.getSrcToTargetOrderMapping();
            Integer srcOrderId = srcToTargetMap.get(edge.getSrcVertexOrder());
            Preconditions.checkArgument(
                    srcOrderId != null,
                    "can not find src vertex order %s in srcToTargetMap %s",
                    edge.getSrcVertexOrder(),
                    srcToTargetMap);
            return new EdgeDataKey(srcOrderId, targetOrderId, edge.getDirection());
        }
    }
}
