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

import com.alibaba.graphscope.common.ir.rel.graph.AbstractBindableTableScan;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasCollector;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.type.GraphNameOrId;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Sarg;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class GraphSelectivityHandler extends RelMdSelectivity
        implements BuiltInMetadata.Selectivity.Handler {
    private static final double FACTOR = 1.2d;

    @Override
    public @Nullable Double getSelectivity(
            RelNode node, RelMetadataQuery mq, @Nullable RexNode condition) {
        if (node instanceof TableScan) {
            return getSelectivity((TableScan) node, mq, condition);
        }
        return RelMdUtil.guessSelectivity(condition);
    }

    @Override
    public Double getSelectivity(TableScan tableScan, RelMetadataQuery mq, RexNode condition) {
        double total = 1.0d;
        if (condition == null || condition.isAlwaysTrue()) return total;
        for (RexNode conjunction : RelOptUtil.conjunctions(condition)) {
            double perSelectivity = 0.0d;
            for (RexNode disjunction : RelOptUtil.disjunctions(conjunction)) {
                perSelectivity += guessSelectivity(tableScan, mq, disjunction);
            }
            total *= perSelectivity;
        }
        return total;
    }

    private double guessSelectivity(TableScan tableScan, RelMetadataQuery mq, RexNode condition) {
        // return the table scan tagged by the alias id in the variable
        RexVariableAliasCollector<Pair> varTableScanCollector =
                new RexVariableAliasCollector<Pair>(
                        true,
                        (RexGraphVariable var) -> {
                            TableScan scanByAlias =
                                    getTableScanByAlias(tableScan, var.getAliasId());
                            Preconditions.checkArgument(
                                    scanByAlias != null,
                                    "can not find table scan for aliasId=" + var.getAliasId());
                            return Pair.of(var, scanByAlias);
                        });
        double maxCountForUniqueKeys = 0.0d;
        double maxCount = 0.0d;
        for (Pair varTableScan : condition.accept(varTableScanCollector)) {
            RexGraphVariable var = (RexGraphVariable) varTableScan.left;
            TableScan scan = (TableScan) varTableScan.right;
            double count = mq.getRowCount(scan);
            if (isUniqueKey(var, scan) && count > maxCountForUniqueKeys) {
                maxCountForUniqueKeys = count;
            }
            if (count > maxCount) {
                maxCount = count;
            }
        }
        if (Double.compare(maxCountForUniqueKeys, 0.0d) != 0) {
            if (condition.isA(SqlKind.SEARCH)) {
                RexNode right = ((RexCall) condition).getOperands().get(1);
                Sarg sarg = ((RexLiteral) right).getValueAs(Sarg.class);
                return sarg.pointCount / maxCountForUniqueKeys;
            } else if (condition.isA(SqlKind.EQUALS)) {
                return 1.0d / maxCountForUniqueKeys;
            }
        }
        return Math.max(RelMdUtil.guessSelectivity(condition), relax(1.0d / maxCount));
    }

    private double relax(double value) {
        double relaxValue = value * FACTOR;
        return Double.compare(relaxValue, 1.0d) > 0 ? 1.0d : relaxValue;
    }

    private boolean isUniqueKey(RexGraphVariable var, RelNode tableScan) {
        if (var.getProperty() == null) return false;
        switch (var.getProperty().getOpt()) {
            case ID:
                return true;
            case KEY:
                GraphSchemaType schemaType =
                        (GraphSchemaType) tableScan.getRowType().getFieldList().get(0).getType();
                ImmutableBitSet propertyIds = getPropertyIds(var.getProperty(), schemaType);
                if (!propertyIds.isEmpty() && tableScan.getTable().isKey(propertyIds)) {
                    return true;
                }
            case LABEL:
            case ALL:
            case LEN:
            default:
                return false;
        }
    }

    private ImmutableBitSet getPropertyIds(GraphProperty property, GraphSchemaType schemaType) {
        if (property.getOpt() != GraphProperty.Opt.KEY) return ImmutableBitSet.of();
        GraphNameOrId key = property.getKey();
        if (key.getOpt() == GraphNameOrId.Opt.ID) {
            return ImmutableBitSet.of(key.getId());
        }
        for (int i = 0; i < schemaType.getFieldList().size(); ++i) {
            RelDataTypeField field = schemaType.getFieldList().get(i);
            if (field.getName().equals(key.getName())) {
                return ImmutableBitSet.of(i);
            }
        }
        return ImmutableBitSet.of();
    }

    private TableScan getTableScanByAlias(RelNode top, int aliasId) {
        List<RelNode> queue = Lists.newArrayList(top);
        while (!queue.isEmpty()) {
            RelNode cur = queue.remove(0);
            if (cur instanceof AbstractBindableTableScan
                    && (aliasId == AliasInference.DEFAULT_ID
                            || ((AbstractBindableTableScan) cur).getAliasId() == aliasId)) {
                return (AbstractBindableTableScan) cur;
            }
            queue.addAll(cur.getInputs());
        }
        return null;
    }
}
