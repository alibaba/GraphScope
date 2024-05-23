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

package com.alibaba.graphscope.cypher.antlr4.visitor;

import com.alibaba.graphscope.common.ir.meta.QueryMode;
import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedures;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.grammar.CypherGSBaseVisitor;
import com.alibaba.graphscope.grammar.CypherGSParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.javatuples.Pair;

import java.util.List;
import java.util.stream.Collectors;

public class ProcedureCallVisitor extends CypherGSBaseVisitor<LogicalPlan> {
    private final GraphBuilder builder;
    private final ExpressionVisitor expressionVisitor;
    private final IrMeta irMeta;

    public ProcedureCallVisitor(GraphBuilder builder, IrMeta irMeta) {
        this.builder = builder;
        this.expressionVisitor = new ExpressionVisitor(new GraphBuilderVisitor(this.builder));
        this.irMeta = irMeta;
    }

    @Override
    public LogicalPlan visitOC_Cypher(CypherGSParser.OC_CypherContext ctx) {
        return visitOC_Statement(ctx.oC_Statement());
    }

    @Override
    public LogicalPlan visitOC_StandaloneCall(CypherGSParser.OC_StandaloneCallContext ctx) {
        if (ctx.oC_ExplicitProcedureInvocation() != null) {
            return visitOC_ExplicitProcedureInvocation(ctx.oC_ExplicitProcedureInvocation());
        } else {
            return visitOC_ImplicitProcedureInvocation(ctx.oC_ImplicitProcedureInvocation());
        }
    }

    @Override
    public LogicalPlan visitOC_ExplicitProcedureInvocation(
            CypherGSParser.OC_ExplicitProcedureInvocationContext ctx) {
        Pair<SqlOperator, QueryMode> operatorModePair =
                visitOC_ProcedureNameAsOperator(ctx.oC_ProcedureName());
        List<RexNode> operands =
                ctx.oC_Expression().stream()
                        .map(this::visitOC_Expression0)
                        .collect(Collectors.toList());
        RexNode procedureCall = builder.call(operatorModePair.getValue0(), operands);
        return new LogicalPlan(procedureCall, operatorModePair.getValue1());
    }

    @Override
    public LogicalPlan visitOC_ImplicitProcedureInvocation(
            CypherGSParser.OC_ImplicitProcedureInvocationContext ctx) {
        Pair<SqlOperator, QueryMode> operatorModePair =
                visitOC_ProcedureNameAsOperator(ctx.oC_ProcedureName());
        RexNode procedureCall = builder.call(operatorModePair.getValue0(), ImmutableList.of());
        return new LogicalPlan(procedureCall, operatorModePair.getValue1());
    }

    // visit procedure name
    private Pair<SqlOperator, QueryMode> visitOC_ProcedureNameAsOperator(
            CypherGSParser.OC_ProcedureNameContext ctx) {
        String procedureName = ctx.getText();
        StoredProcedureMeta meta = null;
        StoredProcedures procedures = irMeta.getStoredProcedures();
        Preconditions.checkArgument(
                procedures != null && (meta = procedures.getStoredProcedure(procedureName)) != null,
                "procedure %s not found",
                procedureName);
        return Pair.with(GraphStdOperatorTable.USER_DEFINED_PROCEDURE(meta), meta.getMode());
    }

    // visit procedure parameters
    private RexNode visitOC_Expression0(CypherGSParser.OC_ExpressionContext ctx) {
        return expressionVisitor.visitOC_Expression(ctx).getExpr();
    }
}
