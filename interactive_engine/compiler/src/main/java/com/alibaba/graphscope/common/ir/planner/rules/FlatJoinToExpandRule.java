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
package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.rel.GraphShuttle;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.AliasNameWithId;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.List;

public class FlatJoinToExpandRule extends FlatJoinRule {
    // left plan of the join
    private RelNode left;
    // alias of the left key in the join condition
    private AliasNameWithId leftAlias;
    // whether the 'MatchOpt' in the right plan is optional
    private boolean optional;
    private List<RexNode> otherJoinConditions;

    private final PlannerConfig config;

    public FlatJoinToExpandRule(PlannerConfig config) {
        this.config = config;
    }

    @Override
    protected boolean matches(LogicalJoin join) {
        RexNode condition = join.getCondition();
        List<RexNode> others = Lists.newArrayList();
        RexGraphVariable var = joinByOneColumn(condition, others);
        if (var == null) return false;
        List<GraphLogicalSingleMatch> matches = Lists.newArrayList();
        getMatchBeforeJoin(join.getRight(), matches);
        if (matches.size() != 1) return false;
        RelNode sentence = matches.get(0).getSentence();
        if (hasPxdWithUntil(sentence)) return false;
        if (hasNodeEqualFilter(sentence)) return false;
        GraphLogicalSource source = getSource(sentence);
        List<Integer> startEndAliasIds =
                Lists.newArrayList(getAliasId(source), getAliasId(sentence));
        // check whether the sentence starts or ends with the alias id in the join condition
        boolean contains = startEndAliasIds.contains(var.getAliasId());
        if (contains) {
            left = join.getLeft();
            leftAlias = new AliasNameWithId(var.getName().split("\\.")[0], var.getAliasId());
            optional = join.getJoinType() == JoinRelType.LEFT;
            otherJoinConditions = others;
        }
        return contains;
    }

    @Override
    protected RelNode perform(LogicalJoin join) {
        return join.getRight().accept(new ExpandFlatter());
    }

    // perform the transformation from the join plan to the expand plan
    // replace the source operator of the right plan with the left plan,
    // i.e. join(getV1->expand1->source1, getV2->expand2->source2) =>
    // getV2->expand2->getV1->expand1->source1
    private class ExpandFlatter extends GraphShuttle {
        @Override
        public RelNode visit(GraphLogicalSingleMatch match) {
            RelNode sentence = match.getSentence();
            // reverse the sentence if the expand order does not start from the 'leftAlias'
            RelNode source = getSource(sentence);
            if (getAliasId(source) != leftAlias.getAliasId()) {
                sentence = sentence.accept(new Reverse(sentence));
            }
            // set expand operators in the right plan as optional if 'optional' = true
            if (optional) {
                sentence = sentence.accept(new SetOptional());
            }
            // replace the source operator of the sentence with the left plan
            RelNode expand = sentence.accept(new ReplaceInput(leftAlias, left));
            // if there are other join conditions, add a filter operator on top of the expand
            // operator
            if (!otherJoinConditions.isEmpty()) {
                RexNode otherJoinCondition =
                        RexUtil.composeConjunction(
                                match.getCluster().getRexBuilder(), otherJoinConditions);
                LogicalFilter filter = LogicalFilter.create(expand, otherJoinCondition);
                expand = filter;
            }
            return expand;
        }
    }
}
