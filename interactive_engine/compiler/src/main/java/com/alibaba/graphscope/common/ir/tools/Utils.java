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

package com.alibaba.graphscope.common.ir.tools;

import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.rel.CommonTableScan;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Sarg;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Utils {
    public static RelDataType getOutputType(RelNode topNode) {
        List<RelDataTypeField> outputFields = Lists.newArrayList();
        List<RelNode> inputs = Lists.newArrayList(topNode);
        while (!inputs.isEmpty()) {
            RelNode cur = inputs.remove(0);
            outputFields.addAll(0, cur.getRowType().getFieldList());
            if (AliasInference.removeAlias(cur)) {
                break;
            }
            inputs.addAll(cur.getInputs());
        }
        Set<String> uniqueNames = Sets.newHashSet();
        // if field name is duplicated, we dedup it and keep the last one
        List<RelDataTypeField> dedup = Lists.newArrayList();
        for (int i = outputFields.size() - 1; i >= 0; i--) {
            RelDataTypeField field = outputFields.get(i);
            // specific implementation for gremlin `head`, DEFAULT can only denote the last field
            if (field.getName() == AliasInference.DEFAULT_NAME && i != outputFields.size() - 1) {
                continue;
            }
            if (!uniqueNames.contains(field.getName())) {
                uniqueNames.add(field.getName());
                dedup.add(0, field);
            }
        }
        return new RelRecordType(StructKind.FULLY_QUALIFIED, dedup);
    }

    public static List<Comparable> getValuesAsList(Comparable value) {
        List<Comparable> values = Lists.newArrayList();
        if (value instanceof NlsString) {
            values.add(((NlsString) value).getValue());
        } else if (value instanceof Sarg) {
            Sarg sarg = (Sarg) value;
            if (sarg.isPoints()) {
                Set<Range<Comparable>> rangeSets = sarg.rangeSet.asRanges();
                for (Range<Comparable> range : rangeSets) {
                    values.addAll(getValuesAsList(range.lowerEndpoint()));
                }
            }
        } else {
            values.add(value);
        }
        return values;
    }

    public static GraphLabelType getGraphLabels(RelDataType rowType) {
        if (rowType instanceof GraphSchemaType) {
            return ((GraphSchemaType) rowType).getLabelType();
        } else {
            List<RelDataTypeField> fields = rowType.getFieldList();
            Preconditions.checkArgument(
                    !fields.isEmpty() && fields.get(0).getType() instanceof GraphSchemaType,
                    "data type of graph operators should be %s ",
                    GraphSchemaType.class);
            GraphSchemaType schemaType = (GraphSchemaType) fields.get(0).getType();
            return schemaType.getLabelType();
        }
    }

    public static String toString(RelNode node, SqlExplainLevel detailLevel) {
        return toString("root:", node, Sets.newHashSet(), detailLevel);
    }

    /**
     * print root {@code RelNode} and nested {@code RelNode}s in each {@code CommonTableScan}
     * @param node
     * @return
     */
    public static String toString(RelNode node) {
        return toString("root:", node, Sets.newHashSet(), SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    }

    private static String toString(
            String header, RelNode node, Set<String> dedup, SqlExplainLevel detailLevel) {
        StringBuilder builder = new StringBuilder();
        if (!header.isEmpty()) {
            dedup.add(header);
            builder.append(header).append("\n");
        }
        builder.append(explain(node, detailLevel));
        List<RelNode> inputs = Lists.newArrayList(node.getInputs());
        while (!inputs.isEmpty()) {
            RelNode input = inputs.remove(0);
            if (input instanceof CommonTableScan) {
                CommonOptTable optTable = (CommonOptTable) ((CommonTableScan) input).getTable();
                String name = optTable.getQualifiedName().get(0) + ":";
                if (!dedup.contains(name)) {
                    builder.append(toString(name, optTable.getCommon(), dedup, detailLevel));
                }
            }
            inputs.addAll(input.getInputs());
        }
        return builder.toString();
    }

    public static @Nullable String explain(@Nullable RelNode rel, SqlExplainLevel detailLevel) {
        if (rel == null) {
            return null;
        } else {
            StringWriter sw = new StringWriter();
            RelWriter planWriter =
                    new RelWriterImpl(new PrintWriter(sw), detailLevel, false) {
                        @Override
                        protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
                            List<RelNode> inputs = rel.getInputs();
                            final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
                            if (!mq.isVisibleInExplain(rel, detailLevel)) {
                                // render children in place of this, at same level
                                explainInputs(inputs);
                                return;
                            }

                            StringBuilder s = new StringBuilder();
                            spacer.spaces(s);
                            if (withIdPrefix) {
                                s.append(rel.getId()).append(":");
                            }
                            s.append(rel.getRelTypeName());
                            if (detailLevel != SqlExplainLevel.NO_ATTRIBUTES) {
                                int j = 0;
                                for (Pair<String, @Nullable Object> value : values) {
                                    if (value.right instanceof RelNode) {
                                        continue;
                                    }
                                    if (j++ == 0) {
                                        s.append("(");
                                    } else {
                                        s.append(", ");
                                    }
                                    s.append(value.left)
                                            .append("=[")
                                            .append(value.right)
                                            .append("]");
                                }
                                if (j > 0) {
                                    s.append(")");
                                }
                            }
                            switch (detailLevel) {
                                case ALL_ATTRIBUTES:
                                    s.append(": rowcount = ")
                                            .append(mq.getRowCount(rel))
                                            .append(", cumulative cost = ")
                                            .append(mq.getCumulativeCost(rel));
                                    break;
                                case NON_COST_ATTRIBUTES:
                                    s.append(": rowcount = ").append(mq.getRowCount(rel));
                                default:
                                    break;
                            }
                            switch (detailLevel) {
                                case NON_COST_ATTRIBUTES:
                                case ALL_ATTRIBUTES:
                                    break;
                                default:
                                    break;
                            }
                            pw.println(s);
                            spacer.add(2);
                            explainInputs(inputs);
                            spacer.subtract(2);
                        }

                        private void explainInputs(List<RelNode> inputs) {
                            Iterator var2 = inputs.iterator();

                            while (var2.hasNext()) {
                                RelNode input = (RelNode) var2.next();
                                input.explain(this);
                            }
                        }
                    };
            rel.explain(planWriter);
            return sw.toString();
        }
    }
}
