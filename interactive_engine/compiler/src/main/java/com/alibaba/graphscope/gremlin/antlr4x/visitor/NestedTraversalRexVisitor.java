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

package com.alibaba.graphscope.gremlin.antlr4x.visitor;

import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.rel.CommonTableScan;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;
import java.util.function.Predicate;

public class NestedTraversalRexVisitor extends GremlinGSBaseVisitor<RexNode> {
    private final GraphBuilder parentBuilder;
    private final GraphBuilder nestedBuilder;
    private final @Nullable String headAlias;
    private final ParserRuleContext parentCtx;

    public NestedTraversalRexVisitor(
            GraphBuilder parentBuilder, @Nullable String headAlias, ParserRuleContext parentCtx) {
        this.parentBuilder = parentBuilder;
        this.nestedBuilder =
                GraphBuilder.create(
                        this.parentBuilder.getContext(),
                        (GraphOptCluster) this.parentBuilder.getCluster(),
                        this.parentBuilder.getRelOptSchema());
        this.headAlias = headAlias;
        this.parentCtx = parentCtx;
    }

    @Override
    public RexNode visitNestedTraversal(GremlinGSParser.NestedTraversalContext ctx) {
        RelNode commonRel =
                Objects.requireNonNull(
                        parentBuilder.peek(), "parentCtx builder should not be empty");
        commonRel =
                new CommonTableScan(
                        commonRel.getCluster(),
                        commonRel.getTraitSet(),
                        new CommonOptTable(commonRel));
        nestedBuilder.push(commonRel);
        if (headAlias != null) {
            nestedBuilder.project(
                    ImmutableList.of(nestedBuilder.variable(headAlias)), ImmutableList.of(), true);
        }
        // set query given alias
        String tailAlias = null;
        int methodCounter = 0;
        TraversalMethodIterator iterator = new TraversalMethodIterator(ctx);
        while (iterator.hasNext()) {
            GremlinGSParser.TraversalMethodContext cur = iterator.next();
            if (methodCounter != 0 && !iterator.hasNext() && cur.traversalMethod_as() != null) {
                tailAlias =
                        (String)
                                LiteralVisitor.INSTANCE.visit(
                                        cur.traversalMethod_as().StringLiteral());
            }
            ++methodCounter;
        }
        TrimAlias trimAlias = new TrimAlias(methodCounter);
        GraphBuilderVisitor visitor = new GraphBuilderVisitor(nestedBuilder, trimAlias);
        // skip head and tail aliases in nested traversal, we have handled them specifically in
        // current context.
        RelNode subRel = visitor.visitNestedTraversal(ctx).build();
        RexNode expr;
        if (new SubQueryChecker(commonRel).test(subRel)) {
            if (parentCtx instanceof GremlinGSParser.TraversalMethod_whereContext) {
                if (tailAlias != null) {
                    // specific implementation for `where(('a').out().as('b')`, convert tail alias
                    // `as('b')` to `where(eq('b'))`
                    subRel =
                            nestedBuilder
                                    .push(subRel)
                                    .filter(
                                            nestedBuilder.equals(
                                                    nestedBuilder.variable((String) null),
                                                    nestedBuilder.variable(tailAlias)))
                                    .build();
                    tailAlias = null;
                }
                expr = RexSubQuery.exists(subRel);
            } else if (parentCtx instanceof GremlinGSParser.TraversalMethod_notContext) {
                expr = nestedBuilder.not(RexSubQuery.exists(subRel)); // convert to not exist
            } else if (parentCtx instanceof GremlinGSParser.TraversalMethod_wherebyContext
                    || parentCtx instanceof GremlinGSParser.TraversalMethod_selectbyContext
                    || parentCtx instanceof GremlinGSParser.TraversalMethod_dedupbyContext
                    || parentCtx instanceof GremlinGSParser.TraversalMethod_orderbyContext
                    || parentCtx instanceof GremlinGSParser.TraversalMethod_group_keybyContext) {
                expr = RexSubQuery.scalar(subRel);
            } else {
                throw new UnsupportedOperationException(
                        "unsupported nested traversal in parent: " + parentCtx.getText());
            }
        } else {
            // convert subRel to expression
            Project project = (Project) subRel;
            Preconditions.checkArgument(
                    project.getProjects().size() == 1,
                    "value returned from sub query should be single value");
            expr = project.getProjects().get(0);
            if (parentCtx instanceof GremlinGSParser.TraversalMethod_whereContext) {
                // convert to IS_NOT_NULL
                expr = nestedBuilder.isNotNull(expr);
            } else if (parentCtx instanceof GremlinGSParser.TraversalMethod_notContext) {
                // convert to IS_NULL
                expr = nestedBuilder.isNull(expr);
            }
        }
        return nestedBuilder.alias(expr, tailAlias);
    }

    private class TrimAlias implements Predicate<ParseTree> {
        private int methodIdx;
        private final int methodCount;

        public TrimAlias(int methodCount) {
            this.methodCount = methodCount;
            this.methodIdx = 0;
        }

        @Override
        public boolean test(ParseTree parseTree) {
            if (parseTree.getParent() instanceof GremlinGSParser.TraversalMethodContext) {
                boolean toSkip =
                        parseTree instanceof GremlinGSParser.TraversalMethod_asContext
                                && (methodIdx == 0 || methodIdx == methodCount - 1);
                ++methodIdx;
                return toSkip;
            } else {
                return false;
            }
        }
    }
}
