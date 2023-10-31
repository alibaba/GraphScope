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

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.planner.type.DataKey;
import com.alibaba.graphscope.common.ir.planner.type.DataValue;
import com.alibaba.graphscope.common.ir.planner.type.EdgeDataKey;
import com.alibaba.graphscope.common.ir.planner.type.VertexDataKey;
import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.GraphRelShuttleX;
import com.alibaba.graphscope.common.ir.rel.graph.AbstractBindableTableScan;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.AbstractLogicalMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class GraphIOProcessor {
    private final Configs configs;
    private final RexBuilder rexBuilder;
    private final Map<DataKey, DataValue> graphDetails;

    public GraphIOProcessor(Configs configs) {
        this.configs = configs;
        this.rexBuilder = GraphPlanner.rexBuilderFactory.apply(configs);
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
        InputConvertor convertor =
                InputConvertor.class.cast(
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class[] {InputConvertor.class},
                                (proxy, method, args) -> {
                                    if (args.length > 0
                                            && AbstractLogicalMatch.class.isAssignableFrom(
                                                    args[0].getClass())) {
                                        return method.invoke(proxy, args);
                                    }
                                    return null;
                                }));
        return convertor.visit(input);
    }

    /**
     * convert {@code Intersect} to logical {@code RelNode}
     * @param output
     * @return
     */
    public RelNode processOutput(RelNode output) {
        return null;
    }

    private abstract class InputConvertor extends GraphRelShuttleX {
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
            Map<Object, DataValue> vertexOrEdgeDetails = Maps.newHashMap();
            RelVisitor visitor =
                    new RelVisitor() {
                        Map<Integer, PatternVertex> aliasIdToVertex = Maps.newHashMap();
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
                        }

                        private PatternVertex visitAndAddVertex(
                                AbstractBindableTableScan tableScan) {
                            int aliasId = tableScan.getAliasId();
                            PatternVertex existVertex = aliasIdToVertex.get(aliasId);
                            if (existVertex == null) {
                                int vertexId = idGenerator.getAndIncrement();
                                List<Integer> typeIds = getTypeIds(tableScan);
                                existVertex =
                                        (typeIds.size() == 1)
                                                ? new SinglePatternVertex(typeIds.get(0), vertexId)
                                                : new FuzzyPatternVertex(typeIds, vertexId);
                                pattern.addVertex(existVertex);
                                if (aliasId != AliasInference.DEFAULT_ID) {
                                    aliasIdToVertex.put(aliasId, existVertex);
                                }
                                vertexOrEdgeDetails.put(
                                        existVertex, new DataValue(aliasId, getFilters(tableScan)));
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
                            List<Integer> typeIds = getTypeIds(expand);
                            List<Integer> srcTypeIds = src.getVertexTypeIds();
                            List<Integer> dstTypeIds = dst.getVertexTypeIds();
                            List<EdgeTypeId> edgeTypeIds = Lists.newArrayList();
                            for (Integer srcType : srcTypeIds) {
                                for (Integer dstType : dstTypeIds) {
                                    for (Integer type : typeIds) {
                                        edgeTypeIds.add(new EdgeTypeId(srcType, dstType, type));
                                    }
                                }
                            }
                            int edgeId = idGenerator.getAndIncrement();
                            PatternEdge edge =
                                    (edgeTypeIds.size() == 1)
                                            ? new SinglePatternEdge(edgeTypeIds.get(0), edgeId)
                                            : new FuzzyPatternEdge(edgeTypeIds, edgeId);
                            pattern.addEdge(src, dst, edge);
                            vertexOrEdgeDetails.put(
                                    edge, new DataValue(expand.getAliasId(), getFilters(expand)));
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
                            // todo: support both
                            key = new EdgeDataKey(srcOrderId, dstOrderId, PatternDirection.OUT);
                        }
                        graphDetails.put(key, v);
                    });
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
            return filters.isEmpty() ? null : RexUtil.composeConjunction(rexBuilder, filters);
        }

        private List<Integer> getTypeIds(RelNode rel) {
            List<RelDataTypeField> fields = rel.getRowType().getFieldList();
            Preconditions.checkArgument(
                    !fields.isEmpty() && fields.get(0) instanceof GraphSchemaType,
                    "graph operator should have graph schema type");
            GraphSchemaType schemaType = (GraphSchemaType) fields.get(0);
            GraphLabelType labelType = schemaType.getLabelType();
            return labelType.getLabelsEntry().stream()
                    .map(k -> k.getLabelId())
                    .collect(Collectors.toList());
        }
    }

    private abstract class OutputConvertor extends GraphRelShuttleX {
        @Override
        public RelNode visit(GraphExtendIntersect intersect) {
            return null;
        }
    }
}
