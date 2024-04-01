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

import com.alibaba.graphscope.common.ir.rel.GraphShuttle;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.tools.AliasIdGenerator;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class GraphLogicalSingleMatch extends AbstractLogicalMatch {
    private final RelNode sentence;
    private final GraphOpt.Match matchOpt;

    protected GraphLogicalSingleMatch(
            GraphOptCluster cluster,
            @Nullable List<RelHint> hints,
            @Nullable RelNode input,
            RelNode sentence,
            GraphOpt.Match matchOpt) {
        super(cluster, hints, input);
        this.sentence = Objects.requireNonNull(sentence);
        this.matchOpt = matchOpt;
    }

    public static GraphLogicalSingleMatch create(
            GraphOptCluster cluster,
            @Nullable List<RelHint> hints,
            RelNode input,
            RelNode sentence,
            GraphOpt.Match matchOpt) {
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
        List<RelDataTypeField> fields = Lists.newArrayList();
        if (input != null) {
            fields.addAll(input.getRowType().getFieldList().stream().collect(Collectors.toList()));
        }
        Set<String> inputUniqueNames = fields.stream().map(k -> k.getName()).collect(Collectors.toSet());
        List<RelDataTypeField> curFields = Lists.newArrayList();
        addFields(curFields, sentence);
        List<RexNode> curVars = curFields.stream().map(k -> RexGraphVariable.of(k.getIndex(), 0, k.getName(), k.getType())).collect(Collectors.toList());
        List<String> curNewNames = AliasInference.inferProject(curVars, Lists.newArrayList(), inputUniqueNames);
        // convert to new fields with new alias names and set type to nullable if matchOpt is optional
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        AliasIdGenerator idGenerator = ((GraphOptCluster) getCluster()).getIdGenerator();
        for (int i = 0; i < curFields.size(); ++i) {
            RelDataTypeField field = curFields.get(i);
            String newName = curNewNames.get(i);
            fields.add(new RelDataTypeFieldImpl(
                    newName,
                    idGenerator.generate(newName),
                    typeFactory.createTypeWithNullability(
                            field.getType(), getMatchOpt() == GraphOpt.Match.OPTIONAL)));
        }
        return new RelRecordType(StructKind.FULLY_QUALIFIED, fields);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }

    @Override
    public GraphLogicalSingleMatch copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new GraphLogicalSingleMatch(
                (GraphOptCluster) getCluster(),
                ImmutableList.of(),
                inputs.get(0),
                sentence,
                matchOpt);
    }

    public RelNode getSentence() {
        return sentence;
    }

    public GraphOpt.Match getMatchOpt() {
        return matchOpt;
    }
}
