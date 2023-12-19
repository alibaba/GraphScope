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

package com.alibaba.graphscope.common.ir.rel;

import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasCollector;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.stream.Collectors;

/**
 * visit a graph relation tree and push filter to the corresponding source
 */
public class PushFilterVisitor extends GraphRelVisitor {
    private final GraphBuilder builder;
    private final RexNode condition;
    private final List<Integer> distinctAliasIds;
    private boolean pushed;

    public PushFilterVisitor(GraphBuilder builder, RexNode condition) {
        this.builder = builder;
        this.condition = condition;
        this.distinctAliasIds =
                condition
                        .accept(new RexVariableAliasCollector<>(true, RexGraphVariable::getAliasId))
                        .stream()
                        .distinct()
                        .collect(Collectors.toList());
    }

    @Override
    public RelNode visit(GraphLogicalSingleMatch match) {
        RelNode sentence = match.getSentence().accept(this);
        if (!sentence.equals(match.getSentence())) {
            return builder.match(sentence, match.getMatchOpt()).build();
        }
        return match;
    }

    @Override
    public RelNode visit(GraphLogicalMultiMatch match) {
        List<RelNode> sentences =
                match.getSentences().stream().map(k -> k.accept(this)).collect(Collectors.toList());
        if (!sentences.equals(match.getSentences())) {
            return builder.match(sentences.get(0), sentences.subList(1, sentences.size())).build();
        }
        return match;
    }

    @Override
    public RelNode visit(GraphLogicalSource source) {
        return fuseFilter(source);
    }

    @Override
    public RelNode visit(GraphLogicalExpand expand) {
        return fuseFilter(visitChildren(expand));
    }

    @Override
    public RelNode visit(GraphLogicalGetV getV) {
        return fuseFilter(visitChildren(getV));
    }

    public boolean isPushed() {
        return pushed;
    }

    private RelNode fuseFilter(RelNode node) {
        if (distinctAliasIds.size() != 1) return node;
        int aliasId = distinctAliasIds.get(0);
        RelDataType rowType = node.getRowType();
        for (RelDataTypeField field : rowType.getFieldList()) {
            if (aliasId != AliasInference.DEFAULT_ID && field.getIndex() == aliasId) {
                pushed = true;
                return builder.push(node).filter(condition).build();
            }
        }
        return node;
    }
}
