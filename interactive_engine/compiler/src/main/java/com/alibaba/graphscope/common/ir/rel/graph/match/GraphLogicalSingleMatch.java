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

import com.alibaba.graphscope.common.ir.tools.config.MatchOpt;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

public class GraphLogicalSingleMatch extends AbstractLogicalMatch {
    private final RelNode sentence;
    private final MatchOpt matchOpt;

    protected GraphLogicalSingleMatch(
            GraphOptCluster cluster,
            @Nullable List<RelHint> hints,
            @Nullable RelNode input,
            RelNode sentence,
            MatchOpt matchOpt) {
        super(cluster, hints, input);
        this.sentence = Objects.requireNonNull(sentence);
        this.matchOpt = matchOpt;
    }

    public static GraphLogicalSingleMatch create(
            GraphOptCluster cluster,
            @Nullable List<RelHint> hints,
            RelNode input,
            RelNode sentence,
            MatchOpt matchOpt) {
        return new GraphLogicalSingleMatch(cluster, hints, input, sentence, matchOpt);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("sentence", RelOptUtil.toString(sentence))
                .item("matchOpt", matchOpt);
    }

    @Override
    public RelDataType deriveRowType() {
        List<RelDataTypeField> fields = new ArrayList<>();
        RelNode node = this.sentence;
        addFields(fields, node.getRowType());
        while (ObjectUtils.isNotEmpty(node.getInputs())) {
            node = node.getInput(0);
            addFields(fields, node.getRowType());
        }
        return new RelRecordType(StructKind.FULLY_QUALIFIED, fields);
    }
}
