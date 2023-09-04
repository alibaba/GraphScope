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

import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.alibaba.graphscope.common.ir.rel.graph.AbstractBindableTableScan;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.grammar.CypherGSBaseVisitor;
import com.alibaba.graphscope.grammar.CypherGSParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.stream.Collectors;

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
            GraphBuilderVisitor builderVisitor = new GraphBuilderVisitor(this.builder);
            RelNode regularQuery =
                    builderVisitor.visitOC_RegularQuery(ctx.oC_RegularQuery()).build();
            ImmutableMap<Integer, String> map =
                    builderVisitor.getExpressionVisitor().getDynamicParams();
            return new LogicalPlan(regularQuery, getParameters(regularQuery, map));
        } else {
            RexNode procedureCall =
                    new ProcedureCallVisitor(this.builder, this.irMeta)
                            .visitOC_StandaloneCall(ctx.oC_StandaloneCall());
            return new LogicalPlan(procedureCall);
        }
    }

    private List<StoredProcedureMeta.Parameter> getParameters(
            RelNode relNode, ImmutableMap<Integer, String> paramsIdToName) {
        List<StoredProcedureMeta.Parameter> params = Lists.newArrayList();
        RexVisitor parameterCollector =
                new RexVisitorImpl(true) {
                    @Override
                    public Void visitDynamicParam(RexDynamicParam dynamicParam) {
                        String paramName = paramsIdToName.get(dynamicParam.getIndex());
                        params.add(
                                new StoredProcedureMeta.Parameter(
                                        paramName, dynamicParam.getType()));
                        return null;
                    }
                };
        RelVisitor relVisitor =
                new RelVisitor() {
                    @Override
                    public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                        super.visit(node, ordinal, parent);
                        if (node instanceof GraphLogicalSingleMatch) {
                            visit(((GraphLogicalSingleMatch) node).getSentence(), 0, null);
                        } else if (node instanceof GraphLogicalMultiMatch) {
                            ((GraphLogicalMultiMatch) node)
                                    .getSentences()
                                    .forEach(s -> visit(s, 0, null));
                        } else if (node instanceof AbstractBindableTableScan) {
                            ImmutableList<RexNode> filters =
                                    ((AbstractBindableTableScan) node).getFilters();
                            if (ObjectUtils.isNotEmpty(filters)) {
                                for (RexNode filter : filters) {
                                    filter.accept(parameterCollector);
                                }
                            }
                        } else if (node instanceof Filter) {
                            RexNode condition = ((Filter) node).getCondition();
                            condition.accept(parameterCollector);
                        } else {
                            // todo: collect parameters from Project or Aggregate
                        }
                    }
                };
        relVisitor.go(relNode);
        return params.stream().distinct().collect(Collectors.toList());
    }
}
