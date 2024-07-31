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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;

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

    /**
     * print root {@code RelNode} and nested {@code RelNode}s in each {@code CommonTableScan}
     * @param node
     * @return
     */
    public static String toString(RelNode node) {
        return toString("root:", node, Sets.newHashSet());
    }

    private static String toString(String header, RelNode node, Set<String> dedup) {
        StringBuilder builder = new StringBuilder();
        if (!header.isEmpty()) {
            dedup.add(header);
            builder.append(header).append("\n");
        }
        builder.append(RelOptUtil.toString(node));
        List<RelNode> inputs = Lists.newArrayList(node.getInputs());
        while (!inputs.isEmpty()) {
            RelNode input = inputs.remove(0);
            if (input instanceof CommonTableScan) {
                CommonOptTable optTable = (CommonOptTable) ((CommonTableScan) input).getTable();
                String name = optTable.getQualifiedName().get(0) + ":";
                if (!dedup.contains(name)) {
                    builder.append(toString(name, optTable.getCommon(), dedup));
                }
            }
            inputs.addAll(input.getInputs());
        }
        return builder.toString();
    }
}
