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
import com.alibaba.graphscope.common.ir.type.GraphNameOrId;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class GraphSelectivityHandler extends RelMdSelectivity
        implements BuiltInMetadata.Selectivity.Handler {

    @Override
    public @Nullable Double getSelectivity(
            RelNode node, RelMetadataQuery mq, @Nullable RexNode condition) {
        if (node instanceof TableScan) {
            return getSelectivity((TableScan) node, mq, condition);
        } else if (node instanceof TableScan) {
            return getSelectivity((TableScan) node, mq, condition);
        } else if (node instanceof Filter) {
            return getSelectivity((Filter) node, mq, condition);
        } else if (node instanceof Aggregate) {
            return getSelectivity((Aggregate) node, mq, condition);
        } else if (node instanceof Sort) {
            return getSelectivity((Sort) node, mq, condition);
        } else if (node instanceof Project) {
            return getSelectivity((Project) node, mq, condition);
        } else if (node instanceof Join) {
            return getSelectivity((Join) node, mq, condition);
        } else if (node instanceof Union) {
            return getSelectivity((Union) node, mq, condition);
        }
        throw new IllegalArgumentException("can not estimate selectivity for the node=" + node);
    }

    @Override
    public Double getSelectivity(TableScan tableScan, RelMetadataQuery mq, RexNode condition) {
        if (condition == null || condition.isAlwaysTrue()) return 1.0d;
        double total = 1.0d;
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
        if (condition.isA(SqlKind.EQUALS)) {
            // return the table scan tagged by the alias id in the variable if the variable is a
            // unique key
            RexVariableAliasCollector<Optional<RelNode>> uniqueKeyTableScans =
                    new RexVariableAliasCollector<Optional<RelNode>>(
                            true,
                            (RexGraphVariable var) -> {
                                if (var.getProperty() == null) return Optional.empty();
                                TableScan scanByAlias =
                                        getTableScanByAlias(tableScan, var.getAliasId());
                                Preconditions.checkArgument(
                                        scanByAlias != null,
                                        "can not find table scan for aliasId=" + var.getAliasId());
                                switch (var.getProperty().getOpt()) {
                                    case ID:
                                        return Optional.of(tableScan);
                                    case KEY:
                                        GraphSchemaType schemaType =
                                                (GraphSchemaType)
                                                        scanByAlias
                                                                .getRowType()
                                                                .getFieldList()
                                                                .get(0)
                                                                .getType();
                                        ImmutableBitSet propertyIds =
                                                getPropertyIds(var.getProperty(), schemaType);
                                        if (!propertyIds.isEmpty()
                                                && scanByAlias.getTable().isKey(propertyIds)) {
                                            return Optional.of(scanByAlias);
                                        }
                                    case LABEL:
                                    case ALL:
                                    case LEN:
                                    default:
                                        return Optional.empty();
                                }
                            });
            List<RelNode> tableScanNotNull =
                    condition.accept(uniqueKeyTableScans).stream()
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(Collectors.toList());
            if (!tableScanNotNull.isEmpty()) {
                double maxCount = 0.0d;
                for (RelNode rel : tableScanNotNull) {
                    double count = mq.getRowCount(rel);
                    if (count > maxCount) {
                        maxCount = count;
                    }
                }
                Preconditions.checkArgument(
                        Double.compare(maxCount, 0.0d) != 0, "maxCount should not be 0");
                return 1.0d / maxCount;
            }
        }
        return RelMdUtil.guessSelectivity(condition);
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
        while (queue.isEmpty()) {
            RelNode cur = queue.remove(0);
            if (cur instanceof AbstractBindableTableScan
                    && ((AbstractBindableTableScan) cur).getAliasId() == aliasId) {
                return (AbstractBindableTableScan) cur;
            }
            queue.addAll(cur.getInputs());
        }
        return null;
    }
}
