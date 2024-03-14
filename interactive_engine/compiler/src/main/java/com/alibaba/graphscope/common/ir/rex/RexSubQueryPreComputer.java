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

package com.alibaba.graphscope.common.ir.rex;

import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RexSubQueryPreComputer extends RexVisitorImpl<RexNode> {
    private final List<RexNode> subQueryNodes;
    private final List<String> subQueryAliases;
    private final Set<String> uniqueAliases;
    private final GraphBuilder builder;

    public RexSubQueryPreComputer(GraphBuilder builder) {
        super(true);
        this.builder = builder;
        this.subQueryNodes = Lists.newArrayList();
        this.subQueryAliases = Lists.newArrayList();
        this.uniqueAliases = AliasInference.getUniqueAliasList(builder.peek(), true);
    }

    public RexNode precompute(RexNode top) {
        switch (top.getKind()) {
            case DESCENDING:
            case AS:
            case NOT:
            case AND:
            case OR:
                List<RexNode> operands =
                        ((RexCall) top)
                                .getOperands().stream()
                                        .map(k -> precompute(k))
                                        .collect(Collectors.toList());
                return builder.call(((RexCall) top).getOperator(), operands);
            default:
                if (top instanceof RexSubQuery) {
                    return top;
                }
                return top.accept(this);
        }
    }

    @Override
    public RexNode visitCall(RexCall call) {
        List<RexNode> results = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            results.add(operand.accept(this));
        }
        return builder.call(call.getOperator(), results);
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
        if (literal.getType().getSqlTypeName() == SqlTypeName.SYMBOL
                && literal.getValue() instanceof TimeUnit) {
            return builder.getRexBuilder()
                    .makeIntervalLiteral(
                            null,
                            new SqlIntervalQualifier(
                                    literal.getValueAs(TimeUnit.class), null, SqlParserPos.ZERO));
        }
        return literal;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        return inputRef;
    }

    @Override
    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
        return dynamicParam;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
        int index = subQueryNodes.indexOf(subQuery);
        String variableName;
        // to avoid duplicated expression projecting
        if (index >= 0) {
            variableName = subQueryAliases.get(index);
        } else {
            subQueryNodes.add(subQuery);
            variableName = inferAlias(subQuery);
            subQueryAliases.add(variableName);
            uniqueAliases.add(variableName);
        }
        /**
         * The {@code variableName} has not been stored by {@link GraphBuilder},
         * so we cannot invoke {@link GraphBuilder#variable(String)} to create a {@code RexGraphVariable} at this point.
         * Here just maintain the necessary info in {@code RexTmpVariable} to create a {@code RexGraphVariable} latter.
         */
        return RexTmpVariable.of(variableName, subQuery.getType());
    }

    public List<RexNode> getSubQueryNodes() {
        return subQueryNodes;
    }

    public List<String> getSubQueryAliases() {
        return subQueryAliases;
    }

    private String inferAlias(RexNode node) {
        List<String> newAliases = new ArrayList<>();
        AliasInference.inferProject(
                ImmutableList.of(node), newAliases, Sets.newHashSet(this.uniqueAliases));
        return newAliases.get(0);
    }
}
