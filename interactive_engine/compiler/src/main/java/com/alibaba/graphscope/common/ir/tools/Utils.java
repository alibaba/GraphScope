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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;

import java.util.List;
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
}
