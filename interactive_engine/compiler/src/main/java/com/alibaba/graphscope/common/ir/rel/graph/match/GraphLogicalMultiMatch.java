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

package com.alibaba.graphscope.common.ir.rel.graph.match;

import com.alibaba.graphscope.common.ir.rel.GraphRelVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.stream.Collectors;

public class GraphLogicalMultiMatch extends AbstractLogicalMatch {
    // sentences should >= 2
    private List<RelNode> sentences;

    protected GraphLogicalMultiMatch(
            GraphOptCluster cluster,
            @Nullable List<RelHint> hints,
            @Nullable RelNode input,
            RelNode firstSentence,
            List<RelNode> otherSentences) {
        super(cluster, hints, input);
        ImmutableList.Builder<RelNode> builder = ImmutableList.builder();
        this.sentences =
                builder.add(Objects.requireNonNull(firstSentence))
                        .addAll(ObjectUtils.requireNonEmpty(otherSentences))
                        .build();
    }

    public static GraphLogicalMultiMatch create(
            GraphOptCluster cluster,
            @Nullable List<RelHint> hints,
            RelNode input,
            RelNode firstSentence,
            List<RelNode> otherSentences) {
        return new GraphLogicalMultiMatch(cluster, hints, input, firstSentence, otherSentences);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        Map<String, String> strMap = new LinkedHashMap<>();
        for (int i = 0; i < sentences.size(); ++i) {
            strMap.put(
                    String.format("s%d", i),
                    String.format("[%s]", RelOptUtil.toString(sentences.get(i))));
        }
        return super.explainTerms(pw).itemIf("sentences", strMap, !ObjectUtils.isEmpty(strMap));
    }

    @Override
    public RelDataType deriveRowType() {
        List<RelDataTypeField> fields = new ArrayList<>();
        for (RelNode node : sentences) {
            addFields(fields, node);
        }
        Set<String> fieldNames = Sets.newHashSet();
        List<RelDataTypeField> dedup =
                fields.stream()
                        .filter(
                                k -> {
                                    boolean notExist = !fieldNames.contains(k.getName());
                                    fieldNames.add(k.getName());
                                    return notExist;
                                })
                        .collect(Collectors.toList());
        return new RelRecordType(StructKind.FULLY_QUALIFIED, dedup);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphRelVisitor) {
            return ((GraphRelVisitor) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }

    public List<RelNode> getSentences() {
        return Collections.unmodifiableList(sentences);
    }
}
