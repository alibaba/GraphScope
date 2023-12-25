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
import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.google.common.collect.*;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
        Set<String> fieldNames = Sets.newHashSet();
        List<RelDataTypeField> dedup =
                outputFields.stream()
                        .filter(
                                k -> {
                                    boolean notExist = !fieldNames.contains(k.getName());
                                    fieldNames.add(k.getName());
                                    return notExist;
                                })
                        .collect(Collectors.toList());
        return new RelRecordType(StructKind.FULLY_QUALIFIED, dedup);
    }

    public static List<Comparable> getValuesAsList(Comparable value) {
        ImmutableList.Builder valueBuilder = ImmutableList.builder();
        if (value instanceof NlsString) {
            valueBuilder.add(((NlsString) value).getValue());
        } else if (value instanceof Sarg) {
            Sarg sarg = (Sarg) value;
            if (sarg.isPoints()) {
                Set<Range<Comparable>> rangeSets = sarg.rangeSet.asRanges();
                for (Range<Comparable> range : rangeSets) {
                    valueBuilder.addAll(getValuesAsList(range.lowerEndpoint()));
                }
            }
        } else {
            valueBuilder.add(value);
        }
        return valueBuilder.build();
    }

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

    public static Map<Integer, Pattern> getAllPatterns(RelNode rel) {
        List<RelNode> queue = Lists.newArrayList(rel);
        Map<Integer, Pattern> patterns = Maps.newLinkedHashMap();
        while (!queue.isEmpty()) {
            RelNode cur = queue.remove(0);
            if (cur instanceof GraphExtendIntersect) {
                Pattern src = ((GraphExtendIntersect) cur).getGlogueEdge().getSrcPattern();
                Pattern dst = ((GraphExtendIntersect) cur).getGlogueEdge().getDstPattern();
                patterns.put(src.getPatternId(), src);
                patterns.put(dst.getPatternId(), dst);
            } else if (cur instanceof GraphPattern) {
                Pattern pattern = ((GraphPattern) cur).getPattern();
                patterns.put(pattern.getPatternId(), pattern);
            }
            queue.addAll(cur.getInputs());
        }
        return patterns;
    }
}
