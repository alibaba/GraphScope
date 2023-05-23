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

import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.grammar.CypherGSBaseVisitor;
import com.alibaba.graphscope.grammar.CypherGSParser;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class LogicalPlanVisitor extends CypherGSBaseVisitor<LogicalPlan> {
    private final GraphBuilder builder;
    private final IrMeta irMeta;

    public LogicalPlanVisitor(GraphBuilder builder, IrMeta irMeta) {
        this.builder = builder;
        this.irMeta = irMeta;
    }

    @Override
    public LogicalPlan visitOC_Cypher(CypherGSParser.OC_CypherContext ctx) {
        return visitOC_Statement(ctx.oC_Statement());
    }

    @Override
    public LogicalPlan visitOC_Query(CypherGSParser.OC_QueryContext ctx) {
        if (ctx.oC_RegularQuery() != null) {
            RelNode regularQuery = new GraphBuilderVisitor(this.builder).visitOC_RegularQuery(ctx.oC_RegularQuery()).build();
            return new LogicalPlan(regularQuery, returnEmpty(regularQuery));
        } else {
            RexNode procedureCall = new ProcedureCallVisitor(this.builder, this.irMeta).visitOC_StandaloneCall(ctx.oC_StandaloneCall());
            return new LogicalPlan(procedureCall);
        }
    }

    private boolean returnEmpty(RelNode relNode) {
        List<RelNode> inputs = Lists.newArrayList(relNode);
        while (!inputs.isEmpty()) {
            RelNode cur = inputs.remove(0);
            if (cur instanceof LogicalValues) {
                return true;
            }
            inputs.addAll(cur.getInputs());
        }
        return false;
    }
}
