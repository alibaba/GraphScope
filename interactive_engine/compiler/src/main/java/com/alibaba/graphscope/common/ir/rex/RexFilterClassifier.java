/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.rex;

import com.alibaba.graphscope.common.ir.rel.graph.AbstractBindableTableScan;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.type.TableConfig;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.Utils;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphNameOrId;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Sarg;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.javatuples.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * We use this class to decouple a complex condition that contains nested {@code OR} and {@code AND} operators into different filtering conditions.
 * There are mainly three types of filtering:
 * 1) filtering based on labels;
 * 2) based on the scan's unique key;
 * 3) querying other properties.
 *
 * For example: for the condition _.~label = 'person' AND _.~id = 1 AND _.name = 'marko',
 * _.~label = 'person', _.~id = 1, and _.name = 'marko' would be decoupled into three different {@code RexNode} structures.
 */
public class RexFilterClassifier extends RexVisitorImpl<RexFilterClassifier.Filter> {
    private final GraphBuilder builder;
    private final @Nullable AbstractBindableTableScan tableScan;

    public RexFilterClassifier(
            GraphBuilder builder, @Nullable AbstractBindableTableScan tableScan) {
        super(true);
        this.builder = builder;
        this.tableScan = tableScan;
    }

    public ClassifiedFilter classify(RexNode condition) {
        Filter filter = condition.accept(this);
        List<RexNode> labelFilters = Lists.newArrayList();
        List<RexNode> uniqueKeyFilters = Lists.newArrayList();
        filter.getSchemaFilters()
                .forEach(
                        k -> {
                            switch (k.getSchemaType()) {
                                case LABEL:
                                    labelFilters.add(k.getFilter());
                                    break;
                                case UNIQUE_KEY:
                                default:
                                    uniqueKeyFilters.add(k.getFilter());
                            }
                        });
        List<RexNode> extraFilters = Lists.newArrayList();
        if (filter.getOtherFilter() != null) {
            extraFilters.add(filter.getOtherFilter());
        }
        List<Comparable> labelValues = Lists.newArrayList();
        labelFilters.forEach(k -> labelValues.addAll(getLabelValues(k)));
        return new ClassifiedFilter(labelFilters, labelValues, uniqueKeyFilters, extraFilters);
    }

    private List<Comparable> getLabelValues(RexNode labelFilter) {
        return labelFilter.accept(new LabelValueCollector());
    }

    @Override
    public Filter visitCall(RexCall call) {
        SqlOperator operator = call.getOperator();
        List<RexNode> operands = call.getOperands();
        switch (operator.getKind()) {
            case AND:
                return conjunctions(call, visitList(operands));
            case OR:
                return disjunctions(call);
            case EQUALS:
            case SEARCH:
                RexVariableAliasCollector<Integer> aliasCollector =
                        new RexVariableAliasCollector<>(
                                true, (RexGraphVariable var) -> var.getAliasId());
                if (isLabelEqualFilter(call)) {
                    Integer tagId = call.accept(aliasCollector).get(0);
                    return new Filter(
                            ImmutableList.of(
                                    new Filter.SchemaFilter(tagId, call, Filter.SchemaType.LABEL)),
                            null);
                } else if (tableScan != null && isUniqueKeyEqualFilter(call)) {
                    Integer tagId = call.accept(aliasCollector).get(0);
                    return new Filter(
                            ImmutableList.of(
                                    new Filter.SchemaFilter(
                                            tagId, call, Filter.SchemaType.UNIQUE_KEY)),
                            null);
                }
            default:
                return new Filter(ImmutableList.of(), call);
        }
    }

    private Filter conjunctions(RexNode original, List<Filter> filters) {
        Map<Pair<Integer, Filter.SchemaType>, RexNode> schemaFilterMap = Maps.newLinkedHashMap();
        List<RexNode> otherFilters = Lists.newArrayList();
        filters.forEach(
                k -> {
                    k.getSchemaFilters()
                            .forEach(
                                    v -> {
                                        Pair<Integer, Filter.SchemaType> key =
                                                Pair.with(v.getTagId(), v.getSchemaType());
                                        RexNode filtering = v.getFilter();
                                        RexNode existing = schemaFilterMap.get(key);
                                        if (existing != null) {
                                            filtering = builder.and(existing, v.getFilter());
                                        }
                                        if (!filtering.equals(existing)) {
                                            schemaFilterMap.put(key, filtering);
                                        }
                                    });
                    if (k.getOtherFilter() != null) {
                        otherFilters.add(k.getOtherFilter());
                    }
                });
        List<Filter.SchemaFilter> andSchemaFilters = Lists.newArrayList();
        schemaFilterMap.forEach(
                (k, v) -> {
                    andSchemaFilters.add(new Filter.SchemaFilter(k.getValue0(), v, k.getValue1()));
                });
        RexNode otherFilter =
                otherFilters.isEmpty()
                        ? null
                        : RexUtil.composeConjunction(builder.getRexBuilder(), otherFilters, false);
        return new Filter(andSchemaFilters, otherFilter);
    }

    private Filter disjunctions(RexCall original) {
        SqlOperator operator = original.getOperator();
        switch (operator.getKind()) {
            case OR:
                List<RexNode> operands = original.getOperands();
                Filter.SchemaFilter schemaFilter = null;
                for (RexNode operand : operands) {
                    if (operand.getKind() != SqlKind.EQUALS
                            && operand.getKind() != SqlKind.SEARCH) {
                        return new Filter(ImmutableList.of(), original);
                    }
                    List<Filter.SchemaFilter> curFilters = operand.accept(this).getSchemaFilters();
                    if (curFilters.size() != 1) {
                        return new Filter(ImmutableList.of(), original);
                    }
                    Filter.SchemaFilter cur = curFilters.get(0);
                    if (schemaFilter == null) {
                        schemaFilter = cur;
                    } else {
                        if (schemaFilter.getTagId() == cur.getTagId()
                                && schemaFilter.getSchemaType() == cur.getSchemaType()) {
                            schemaFilter =
                                    new Filter.SchemaFilter(
                                            schemaFilter.getTagId(),
                                            builder.or(schemaFilter.getFilter(), cur.getFilter()),
                                            schemaFilter.getSchemaType());
                        } else {
                            return new Filter(ImmutableList.of(), original);
                        }
                    }
                }
            default:
                return new Filter(ImmutableList.of(), original);
        }
    }

    // check the condition if it is the pattern of label equal filter, i.e. ~label = 'person' or
    // ~label within ['person', 'software']
    // if it is then return the literal containing label values, otherwise null
    private boolean isLabelEqualFilter(RexCall rexCall) {
        return isLabelEqualFilter0(rexCall) != null;
    }

    // check the condition if it is the pattern of label equal filter, i.e. ~label = 'person' or
    // ~label within ['person', 'software']
    // if it is then return the literal containing label values, otherwise null
    private static @Nullable RexLiteral isLabelEqualFilter0(RexNode condition) {
        if (condition instanceof RexCall) {
            RexCall rexCall = (RexCall) condition;
            SqlOperator operator = rexCall.getOperator();
            switch (operator.getKind()) {
                case EQUALS:
                case SEARCH:
                    RexNode left = rexCall.getOperands().get(0);
                    RexNode right = rexCall.getOperands().get(1);
                    if (left.getType() instanceof GraphLabelType && right instanceof RexLiteral) {
                        Comparable value = ((RexLiteral) right).getValue();
                        // if Sarg is a continuous range then the filter is not the 'equal', i.e.
                        // ~label SEARCH [[1, 10]] which means ~label >= 1 and ~label <= 10
                        if (value instanceof Sarg && !((Sarg) value).isPoints()) {
                            return null;
                        }
                        return (RexLiteral) right;
                    } else if (right.getType() instanceof GraphLabelType
                            && left instanceof RexLiteral) {
                        Comparable value = ((RexLiteral) left).getValue();
                        if (value instanceof Sarg && !((Sarg) value).isPoints()) {
                            return null;
                        }
                        return (RexLiteral) left;
                    }
                default:
                    return null;
            }
        } else {
            return null;
        }
    }

    private boolean isUniqueKeyEqualFilter(RexNode condition) {
        if (!(tableScan instanceof GraphLogicalSource)) return false;
        if (condition instanceof RexCall) {
            RexCall rexCall = (RexCall) condition;
            SqlOperator operator = rexCall.getOperator();
            switch (operator.getKind()) {
                case EQUALS:
                case SEARCH:
                    RexNode left = rexCall.getOperands().get(0);
                    RexNode right = rexCall.getOperands().get(1);
                    if (isUniqueKey(left, tableScan) && isLiteralOrDynamicParams(right)) {
                        if (right instanceof RexLiteral) {
                            Comparable value = ((RexLiteral) right).getValue();
                            // if Sarg is a continuous range then the filter is not the 'equal',
                            // i.e. ~id SEARCH [[1, 10]] which means ~id >= 1 and ~id <= 10
                            if (value instanceof Sarg && !((Sarg) value).isPoints()) {
                                return false;
                            }
                        }
                        return true;
                    } else if (isUniqueKey(right, tableScan) && isLiteralOrDynamicParams(left)) {
                        if (left instanceof RexLiteral) {
                            Comparable value = ((RexLiteral) left).getValue();
                            if (value instanceof Sarg && !((Sarg) value).isPoints()) {
                                return false;
                            }
                        }
                        return true;
                    }
                default:
                    return false;
            }
        } else {
            return false;
        }
    }

    private boolean isUniqueKey(RexNode rexNode, RelNode tableScan) {
        if (rexNode instanceof RexGraphVariable) {
            return isUniqueKey((RexGraphVariable) rexNode, tableScan);
        }
        return false;
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
                TableConfig tableConfig = ((AbstractBindableTableScan) tableScan).getTableConfig();
                if (!propertyIds.isEmpty()
                        && tableConfig.getTables().stream().allMatch(k -> k.isKey(propertyIds))) {
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

    private boolean isLiteralOrDynamicParams(RexNode node) {
        return node instanceof RexLiteral || node instanceof RexDynamicParam;
    }

    private static class LabelValueCollector extends RexVisitorImpl<List<Comparable>> {
        public LabelValueCollector() {
            super(true);
        }

        @Override
        public List<Comparable> visitCall(RexCall call) {
            SqlOperator operator = call.getOperator();
            switch (operator.getKind()) {
                case AND:
                    List<Comparable> andLabels = Lists.newArrayList();
                    call.getOperands()
                            .forEach(
                                    k -> {
                                        List<Comparable> cur = k.accept(this);
                                        if (andLabels.isEmpty()) {
                                            andLabels.addAll(cur);
                                        } else {
                                            andLabels.retainAll(cur);
                                        }
                                        if (andLabels.isEmpty()) {
                                            throw new IllegalArgumentException(
                                                    "cannot find common labels between values="
                                                            + andLabels
                                                            + " and values="
                                                            + cur);
                                        }
                                    });
                case OR:
                    List<Comparable> orLabels = Lists.newArrayList();
                    call.getOperands()
                            .forEach(
                                    k -> {
                                        orLabels.addAll(k.accept(this));
                                    });
                    return orLabels.stream().distinct().collect(Collectors.toList());
                case EQUALS:
                case SEARCH:
                    RexLiteral labelLiteral = isLabelEqualFilter0(call);
                    if (labelLiteral != null) {
                        return Utils.getValuesAsList(labelLiteral.getValueAs(Comparable.class));
                    }
                default:
                    return ImmutableList.of();
            }
        }
    }

    // Here we further differentiate the filter conditions for different tags to prevent the
    // grouping of label conditions from various tags.
    // For example, _.~label == 'person' OR _.~label = 'software' can be organized into _.~label IN
    // ['person', 'software'],
    // but a.~label = 'person' OR b.~label = 'software' cannot be.
    public static class Filter {
        private final List<SchemaFilter> schemaFilters;
        private final @Nullable RexNode otherFilter;

        public Filter(List<SchemaFilter> schemaFilters, RexNode otherFilter) {
            this.schemaFilters = schemaFilters;
            this.otherFilter = otherFilter;
        }

        public List<SchemaFilter> getSchemaFilters() {
            return Collections.unmodifiableList(schemaFilters);
        }

        public @Nullable RexNode getOtherFilter() {
            return otherFilter;
        }

        public static class SchemaFilter {
            private final Integer tagId;
            private final RexNode filter;
            private final SchemaType schemaType;

            public SchemaFilter(Integer tagId, RexNode filtering, SchemaType schemaType) {
                this.tagId = tagId;
                this.filter = filtering;
                this.schemaType = schemaType;
            }

            public Integer getTagId() {
                return tagId;
            }

            public RexNode getFilter() {
                return filter;
            }

            public SchemaType getSchemaType() {
                return schemaType;
            }
        }

        public enum SchemaType {
            LABEL,
            UNIQUE_KEY
        }
    }
}
