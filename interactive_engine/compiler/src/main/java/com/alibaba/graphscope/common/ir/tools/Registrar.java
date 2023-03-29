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

import com.alibaba.graphscope.common.ir.rex.RexTmpVariable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Collects the extra expressions which are hard to compute directly by {@code Aggregate} or {@code Sort}.
 * They are needed to be projected in advance, and aliased according to {@link #extraAliases}
 */
public class Registrar {
    private final GraphBuilder builder;

    private List<RexNode> extraNodes;
    private List<String> extraAliases;
    private boolean isAppend;

    private Set<String> uniqueNames;

    public Registrar(GraphBuilder builder, RelNode input, boolean isAppend) {
        this.builder = builder;
        this.extraNodes = new ArrayList<>();
        this.extraAliases = new ArrayList<>();
        this.isAppend = isAppend;
        this.uniqueNames = AliasInference.getUniqueAliasList(input, isAppend);
    }

    public List<RexNode> registerExpressions(List<RexNode> nodes) {
        return nodes.stream().map(k -> registerExpression(k)).collect(Collectors.toList());
    }

    /**
     * for each complex expression (i.e. a.age + 1), the function will put it into {@link #extraNodes}
     * which is the collection of expressions needed to be projected in advance, and generate a new alias name (record in {@link #extraAliases}) for it.
     * The following operators ({@code Aggregate} or {@code Sort}) will depend on the new alias name to refer to the computed value of the expression.
     * @param node
     * @return
     */
    private RexNode registerExpression(RexNode node) {
        // return directly if the node is already a variable
        if (node instanceof RexInputRef) {
            return node;
        }
        switch (node.getKind()) {
            case AS:
                RexNode left = ((RexCall) node).getOperands().get(0);
                RexNode right = ((RexCall) node).getOperands().get(1);
                return builder.alias(registerExpression(left), RexLiteral.stringValue(right));
            case DESCENDING:
                RexNode operand = ((RexCall) node).getOperands().get(0);
                return builder.desc(registerExpression(operand));
            default:
                int index = extraNodes.indexOf(node);
                String variableName;
                // to avoid duplicated expression projecting
                // i.e. group by a.age + 1, sum(a.age + 1), 'a.age + 1' is projected only once
                if (index >= 0) {
                    variableName = extraAliases.get(index);
                } else {
                    extraNodes.add(node);
                    variableName = inferAlias(node);
                    extraAliases.add(variableName);
                    uniqueNames.add(variableName);
                }
                /**
                 * The {@code variableName} has not been stored by {@link GraphBuilder},
                 * so we cannot invoke {@link GraphBuilder#variable(String)} to create a {@code RexGraphVariable} at this point.
                 * Here just maintain the necessary info in {@code RexTmpVariable} to create a {@code RexGraphVariable} latter.
                 */
                return RexTmpVariable.of(variableName, node.getType());
        }
    }

    private String inferAlias(RexNode node) {
        List<String> newAliases = new ArrayList<>();
        AliasInference.inferProject(
                ImmutableList.of(node), newAliases, Sets.newHashSet(this.uniqueNames));
        return newAliases.get(0);
    }

    public List<RexNode> getExtraNodes() {
        return Collections.unmodifiableList(extraNodes);
    }

    public List<String> getExtraAliases() {
        return Collections.unmodifiableList(extraAliases);
    }

    public boolean isAppend() {
        return isAppend;
    }
}
