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

package com.alibaba.graphscope.common.ir.planner.volcano;

import com.alibaba.graphscope.common.ir.planner.type.DataKey;
import com.alibaba.graphscope.common.ir.planner.type.DataValue;
import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.GraphRelShuttle;
import com.alibaba.graphscope.common.ir.rel.graph.AbstractBindableTableScan;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.type.TableConfig;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.gremlin.Utils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.neo4j.cypher.internal.expressions.functions.Abs;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExtVolcanoPlanner extends VolcanoPlanner {
    private final Map<DataKey, DataValue> graphDetails;
    private abstract class InputConvertor implements GraphRelShuttle {
        /**
         * convert match sentences to {@code Pattern}
         * @param match
         * @return
         */
        @Override
        public RelNode visit(GraphLogicalMultiMatch match) {
        }

        @Override
        public RelNode visit(GraphLogicalSingleMatch match) {

        }

        private GraphPattern visit(List<RelNode> sentences) {
            Pattern pattern = new Pattern();
            PatternVertex visited = null;
            Map<Object, DataValue> vertexOrEdgeDetails = Maps.newHashMap();
            RelVisitor traverser = new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                    super.visit(node, ordinal, parent);
                    if (node instanceof AbstractBindableTableScan) {

                    }
                }
            }
            pattern.reordering();
        }

        private RexNode getFilters(AbstractBindableTableScan tableScan) {
            List<RexNode> filters = Lists.newArrayList();
            if (tableScan instanceof GraphLogicalSource) {
                filters.add(((GraphLogicalSource) tableScan)
            }
        }

        private List<Integer> getTypeIds(RelNode rel) {
            List<RelDataTypeField> fields = rel.getRowType().getFieldList();
            Preconditions.checkArgument(!fields.isEmpty() && fields.get(0) instanceof GraphSchemaType, "graph operator should have graph schema type");
            GraphSchemaType schemaType = (GraphSchemaType) fields.get(0);
            GraphLabelType labelType = schemaType.getLabelType();
            return labelType.getLabelsEntry().stream().map(k -> k.getLabelId()).collect(Collectors.toList());
        }
    }

    private interface OutputConvertor extends GraphRelShuttle {
        /**
         * convert intersect to logical {@code RelNode}
         * @param intersect
         * @return
         */
        default RelNode visit(GraphExtendIntersect intersect) {
        }
    }

    public ExtVolcanoPlanner() {
        super();
        graphDetails = Maps.newHashMap();
    }

    @Override
    protected RelOptCost upperBoundForInputs(RelNode mExpr, RelOptCost upperBound) {
        RelSubset group = getSubset(mExpr);
        RelOptCost bestCost = (group != null) ? Utils.getFieldValue(RelSubset.class, group, "bestCost") : null;
        RelOptCost currentUpperBound =
                (bestCost == null || upperBound != null && upperBound.isLt(bestCost))
                        ? upperBound
                        : bestCost;
        if (currentUpperBound != null && !currentUpperBound.isInfinite()) {
            RelOptCost rootCost = mExpr.getCluster().getMetadataQuery().getNonCumulativeCost(mExpr);
            if (rootCost != null && !rootCost.isInfinite()) {
                return currentUpperBound.minus(rootCost);
            }
        }
        return upperBound;
    }

    @Override
    public void setRoot(RelNode rel) {
        super.setRoot(rel);
    }

    @Override
    public RelNode findBestExp() {
        return super.findBestExp();
    }
}
