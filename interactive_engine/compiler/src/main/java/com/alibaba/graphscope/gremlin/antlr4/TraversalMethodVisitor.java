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

package com.alibaba.graphscope.gremlin.antlr4;

import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.alibaba.graphscope.gremlin.antlr4x.visitor.LiteralList;
import com.alibaba.graphscope.gremlin.antlr4x.visitor.LiteralVisitor;
import com.alibaba.graphscope.gremlin.exception.InvalidGremlinScriptException;
import com.alibaba.graphscope.gremlin.exception.UnsupportedEvalException;
import com.alibaba.graphscope.gremlin.plugin.step.ExprStep;
import com.alibaba.graphscope.gremlin.plugin.step.PathExpandStep;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversal;
import com.alibaba.graphscope.gremlin.plugin.type.AnyValue;
import com.google.common.base.Preconditions;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.List;

public class TraversalMethodVisitor extends TraversalRootVisitor<GraphTraversal> {
    final GraphTraversal graphTraversal;

    public TraversalMethodVisitor(
            final GremlinGSBaseVisitor<GraphTraversalSource> gvisitor,
            final GraphTraversal graphTraversal) {
        super(gvisitor);
        this.graphTraversal = graphTraversal;
    }

    @Override
    public Traversal visitTraversalMethod(GremlinGSParser.TraversalMethodContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Traversal visitTraversalMethod_hasLabel(
            GremlinGSParser.TraversalMethod_hasLabelContext ctx) {
        List<String> labels =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        Preconditions.checkArgument(
                !labels.isEmpty(), "there should be at least one label parameter in `hasLabel`");
        return graphTraversal.hasLabel(
                labels.get(0), labels.subList(1, labels.size()).toArray(new String[0]));
    }

    @Override
    public Traversal visitTraversalMethod_hasId(GremlinGSParser.TraversalMethod_hasIdContext ctx) {
        List<Number> ids =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(Number.class);
        Preconditions.checkArgument(
                !ids.isEmpty(), "there should be at least one id parameter in `hasId`");
        return graphTraversal.hasId(ids.toArray(new Number[0]));
    }

    @Override
    public Traversal visitTraversalMethod_has(GremlinGSParser.TraversalMethod_hasContext ctx) {
        String notice =
                "supported pattern is [has('key', 'value')] or [has('key', P)] or [has('label',"
                        + " 'key', 'value')] or [has('label', 'key', P)]";
        int childCount = ctx.getChildCount();
        if (childCount == 6 && ctx.oC_Literal() != null) {
            return graphTraversal.has(
                    (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral(0)),
                    LiteralVisitor.INSTANCE.visit(ctx.oC_Literal()));
        } else if (childCount == 6 && ctx.traversalPredicate() != null) {
            return graphTraversal.has(
                    (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral(0)),
                    TraversalPredicateVisitor.getInstance()
                            .visitTraversalPredicate(ctx.traversalPredicate()));
        } else if (childCount == 8 && ctx.oC_Literal() != null) {
            return graphTraversal.has(
                    (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral(0)),
                    (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral(1)),
                    LiteralVisitor.INSTANCE.visit(ctx.oC_Literal()));
        } else if (childCount == 8 && ctx.traversalPredicate() != null) {
            return graphTraversal.has(
                    (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral(0)),
                    (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral(1)),
                    TraversalPredicateVisitor.getInstance()
                            .visitTraversalPredicate(ctx.traversalPredicate()));
        } else if (childCount == 4 && ctx.StringLiteral() != null) {
            P eqAny = P.eq(AnyValue.INSTANCE);
            return graphTraversal.has(
                    (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral(0)), eqAny);
        } else {
            throw new UnsupportedEvalException(ctx.getClass(), notice);
        }
    }

    @Override
    public Traversal visitTraversalMethod_hasNot(
            GremlinGSParser.TraversalMethod_hasNotContext ctx) {
        P neqAny = P.neq(AnyValue.INSTANCE);
        return graphTraversal.has(
                (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()), neqAny);
    }

    @Override
    public Traversal visitTraversalMethod_is(GremlinGSParser.TraversalMethod_isContext ctx) {
        if (ctx.oC_Literal() != null) {
            return graphTraversal.is(LiteralVisitor.INSTANCE.visit(ctx.oC_Literal()));
        } else if (ctx.traversalPredicate() != null) {
            return graphTraversal.is(
                    TraversalPredicateVisitor.getInstance()
                            .visitTraversalPredicate(ctx.traversalPredicate()));
        } else {
            throw new UnsupportedEvalException(
                    ctx.getClass(), "supported pattern is [is(27)] or [is(eq(27))])");
        }
    }

    @Override
    public Traversal visitTraversalMethod_out(GremlinGSParser.TraversalMethod_outContext ctx) {
        List<String> labels =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        if (!labels.isEmpty() && isRangeExpression(labels.get(0))) {
            GraphTraversal nestedRange = GremlinAntlrToJava.getTraversalSupplier().get();
            String[] ranges = labels.get(0).split("\\.\\.");
            RangeGlobalStep rangeStep =
                    new RangeGlobalStep(
                            nestedRange.asAdmin(),
                            Integer.valueOf(ranges[0]),
                            Integer.valueOf(ranges[1]));
            nestedRange.asAdmin().addStep(rangeStep);
            IrCustomizedTraversal traversal = (IrCustomizedTraversal) graphTraversal;
            return traversal.out(
                    nestedRange, labels.subList(1, labels.size()).toArray(new String[0]));
        } else {
            return graphTraversal.out(labels.toArray(new String[0]));
        }
    }

    private boolean isRangeExpression(String label) {
        return label.matches("^\\d+\\.\\.\\d+");
    }

    @Override
    public Traversal visitTraversalMethod_in(GremlinGSParser.TraversalMethod_inContext ctx) {
        List<String> labels =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        if (!labels.isEmpty() && isRangeExpression(labels.get(0))) {
            GraphTraversal nestedRange = GremlinAntlrToJava.getTraversalSupplier().get();
            String[] ranges = labels.get(0).split("\\.\\.");
            RangeGlobalStep rangeStep =
                    new RangeGlobalStep(
                            nestedRange.asAdmin(),
                            Integer.valueOf(ranges[0]),
                            Integer.valueOf(ranges[1]));
            nestedRange.asAdmin().addStep(rangeStep);
            IrCustomizedTraversal traversal = (IrCustomizedTraversal) graphTraversal;
            return traversal.in(
                    nestedRange, labels.subList(1, labels.size()).toArray(new String[0]));
        } else {
            return graphTraversal.in(labels.toArray(new String[0]));
        }
    }

    @Override
    public Traversal visitTraversalMethod_both(GremlinGSParser.TraversalMethod_bothContext ctx) {
        List<String> labels =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        if (!labels.isEmpty() && isRangeExpression(labels.get(0))) {
            GraphTraversal nestedRange = GremlinAntlrToJava.getTraversalSupplier().get();
            String[] ranges = labels.get(0).split("\\.\\.");
            RangeGlobalStep rangeStep =
                    new RangeGlobalStep(
                            nestedRange.asAdmin(),
                            Integer.valueOf(ranges[0]),
                            Integer.valueOf(ranges[1]));
            nestedRange.asAdmin().addStep(rangeStep);
            IrCustomizedTraversal traversal = (IrCustomizedTraversal) graphTraversal;
            return traversal.both(
                    nestedRange, labels.subList(1, labels.size()).toArray(new String[0]));
        } else {
            return graphTraversal.both(labels.toArray(new String[0]));
        }
    }

    @Override
    public Traversal visitTraversalMethod_outE(GremlinGSParser.TraversalMethod_outEContext ctx) {
        List<String> labels =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        if (ctx.traversalMethod_inV() != null) {
            return graphTraversal.out(labels.toArray(new String[0]));
        } else {
            return graphTraversal.outE(labels.toArray(new String[0]));
        }
    }

    @Override
    public Traversal visitTraversalMethod_inE(GremlinGSParser.TraversalMethod_inEContext ctx) {
        List<String> labels =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        if (ctx.traversalMethod_outV() != null) {
            return graphTraversal.in(labels.toArray(new String[0]));
        } else {
            return graphTraversal.inE(labels.toArray(new String[0]));
        }
    }

    @Override
    public Traversal visitTraversalMethod_bothE(GremlinGSParser.TraversalMethod_bothEContext ctx) {
        List<String> labels =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        if (ctx.traversalMethod_otherV() != null) {
            return graphTraversal.both(labels.toArray(new String[0]));
        } else {
            return graphTraversal.bothE(labels.toArray(new String[0]));
        }
    }

    @Override
    public Traversal visitTraversalMethod_limit(GremlinGSParser.TraversalMethod_limitContext ctx) {
        Number integer = (Number) LiteralVisitor.INSTANCE.visit(ctx.oC_IntegerLiteral());
        return graphTraversal.limit(integer.longValue());
    }

    @Override
    public Traversal visitTraversalMethod_outV(GremlinGSParser.TraversalMethod_outVContext ctx) {
        return graphTraversal.outV();
    }

    @Override
    public Traversal visitTraversalMethod_inV(GremlinGSParser.TraversalMethod_inVContext ctx) {
        return graphTraversal.inV();
    }

    @Override
    public Traversal visitTraversalMethod_endV(GremlinGSParser.TraversalMethod_endVContext ctx) {
        Step endStep = graphTraversal.asAdmin().getEndStep();
        if (endStep == null || !(endStep instanceof PathExpandStep)) {
            throw new InvalidGremlinScriptException(
                    "endV should follow a path expand operator [out('$1..$2'), in('$1..$2'),"
                            + " both('$1..$2')]");
        }
        IrCustomizedTraversal traversal = (IrCustomizedTraversal) graphTraversal;
        return traversal.endV();
    }

    @Override
    public Traversal visitTraversalMethod_otherV(
            GremlinGSParser.TraversalMethod_otherVContext ctx) {
        return graphTraversal.otherV();
    }

    @Override
    public Traversal visitTraversalMethod_valueMap(
            GremlinGSParser.TraversalMethod_valueMapContext ctx) {
        List<String> properties =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        return graphTraversal.valueMap(properties.toArray(new String[0]));
    }

    @Override
    public Traversal visitTraversalMethod_elementMap(
            GremlinGSParser.TraversalMethod_elementMapContext ctx) {
        List<String> properties =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        return graphTraversal.elementMap(properties.toArray(new String[0]));
    }

    @Override
    public Traversal visitTraversalMethod_select(
            GremlinGSParser.TraversalMethod_selectContext ctx) {
        LiteralList literalList = new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression());
        if (!literalList.isEmpty()) {
            List<String> selectTags = literalList.toList(String.class);
            Preconditions.checkArgument(
                    !selectTags.isEmpty(), "there should be at least one select tag in `select`");
            if (selectTags.size() == 1) {
                graphTraversal.select(selectTags.get(0));
            } else {
                graphTraversal.select(
                        selectTags.get(0),
                        selectTags.get(1),
                        selectTags.subList(2, selectTags.size()).toArray(new String[0]));
            }
        } else if (ctx.traversalColumn() != null) {
            Column column =
                    TraversalEnumParser.parseTraversalEnumFromContext(
                            Column.class, ctx.traversalColumn());
            graphTraversal.select(column);
        } else if (ctx.traversalMethod_expr() != null) {
            visitExpr(ctx.traversalMethod_expr(), ExprStep.Type.PROJECTION);
        } else {
            throw new InvalidGremlinScriptException(
                    ctx.getText()
                            + " is invalid, supported pattern is [select('key')] or [select('key1',"
                            + " 'key2', ...)] or [select(Column.keys)] or [select(expr)]");
        }
        // set by traversal
        if (ctx.traversalMethod_selectby_list() != null) {
            ByModulating step = (ByModulating) graphTraversal.asAdmin().getEndStep();
            int childCount = ctx.traversalMethod_selectby_list().getChildCount();
            for (int i = 0; i < childCount; ++i) {
                GremlinGSParser.TraversalMethod_selectbyContext byCtx =
                        ctx.traversalMethod_selectby_list().traversalMethod_selectby(i);
                if (byCtx == null) continue;
                int byChildCount = byCtx.getChildCount();
                // select(..).by()
                if (byChildCount == 3) {
                    step.modulateBy();
                } else if (byChildCount == 4
                        && byCtx.StringLiteral() != null) { // select(..).by('name')
                    step.modulateBy((String) LiteralVisitor.INSTANCE.visit(byCtx.StringLiteral()));
                } else if (byChildCount == 4
                        && byCtx.traversalToken() != null) { // select(..).by(T.label/T.id)
                    step.modulateBy(
                            TraversalEnumParser.parseTraversalEnumFromContext(
                                    T.class, byCtx.traversalToken()));
                } else if (byCtx.traversalMethod_valueMap()
                        != null) { // select(..).by(valueMap('name'))
                    TraversalMethodVisitor nestedVisitor =
                            new TraversalMethodVisitor(
                                    gvisitor, GremlinAntlrToJava.getTraversalSupplier().get());
                    Traversal nestedTraversal =
                            nestedVisitor.visitTraversalMethod_valueMap(
                                    byCtx.traversalMethod_valueMap());
                    step.modulateBy(nestedTraversal.asAdmin());
                } else if (byCtx.traversalMethod_elementMap()
                        != null) { // select(..).by(elementMap('name'))
                    TraversalMethodVisitor nestedVisitor =
                            new TraversalMethodVisitor(
                                    gvisitor, GremlinAntlrToJava.getTraversalSupplier().get());
                    Traversal nestedTraversal =
                            nestedVisitor.visitTraversalMethod_elementMap(
                                    byCtx.traversalMethod_elementMap());
                    step.modulateBy(nestedTraversal.asAdmin());
                } else if (byChildCount == 4
                        && byCtx.nestedTraversal() != null) { // select(..).by(out().count())
                    Traversal nestedTraversal = visitNestedTraversal(byCtx.nestedTraversal());
                    step.modulateBy(nestedTraversal.asAdmin());
                }
            }
        }
        return graphTraversal;
    }

    @Override
    public Traversal visitTraversalMethod_order(GremlinGSParser.TraversalMethod_orderContext ctx) {
        OrderGlobalStep step = (OrderGlobalStep) graphTraversal.order().asAdmin().getEndStep();
        // set order().by(...)
        if (ctx.traversalMethod_orderby_list() != null) {
            int childCount = ctx.traversalMethod_orderby_list().getChildCount();
            for (int i = 0; i < childCount; ++i) {
                GremlinGSParser.TraversalMethod_orderbyContext byCtx =
                        ctx.traversalMethod_orderby_list().traversalMethod_orderby(i);
                if (byCtx == null) continue;
                Order orderOpt = null;
                String strAsKey = null;
                Traversal nestedTraversalAskey = null;
                int byChildCount = byCtx.getChildCount();
                if (byChildCount == 3) {
                    orderOpt = Order.asc;
                }
                if (byCtx.traversalOrder() != null) {
                    orderOpt =
                            TraversalEnumParser.parseTraversalEnumFromContext(
                                    Order.class, byCtx.traversalOrder());
                }
                if (byCtx.StringLiteral() != null) {
                    strAsKey = (String) LiteralVisitor.INSTANCE.visit(byCtx.StringLiteral());
                }
                if (byCtx.traversalMethod_values() != null) {
                    TraversalMethodVisitor nestedVisitor =
                            new TraversalMethodVisitor(
                                    gvisitor, GremlinAntlrToJava.getTraversalSupplier().get());
                    nestedTraversalAskey =
                            nestedVisitor.visitTraversalMethod_values(
                                    byCtx.traversalMethod_values());
                }
                if (byCtx.traversalMethod_select() != null) {
                    TraversalMethodVisitor nestedVisitor =
                            new TraversalMethodVisitor(
                                    gvisitor, GremlinAntlrToJava.getTraversalSupplier().get());
                    nestedTraversalAskey =
                            nestedVisitor.visitTraversalMethod_select(
                                    byCtx.traversalMethod_select());
                }
                if (byCtx.nestedTraversal() != null) {
                    nestedTraversalAskey = visitNestedTraversal(byCtx.nestedTraversal());
                }
                if (strAsKey != null && orderOpt == null) {
                    step.modulateBy(strAsKey);
                } else if (strAsKey != null && orderOpt != null) {
                    step.modulateBy(strAsKey, orderOpt);
                } else if (nestedTraversalAskey != null && orderOpt == null) {
                    step.modulateBy(nestedTraversalAskey.asAdmin());
                } else if (nestedTraversalAskey != null && orderOpt != null) {
                    step.modulateBy(nestedTraversalAskey.asAdmin(), orderOpt);
                } else if (orderOpt != null) {
                    step.modulateBy(orderOpt);
                } else {
                    throw new UnsupportedEvalException(
                            byCtx.getClass(), "pattern of order by is unsupported");
                }
            }
        }
        return graphTraversal;
    }

    @Override
    public Traversal visitTraversalMethod_as(GremlinGSParser.TraversalMethod_asContext ctx) {
        if (ctx.getChildCount() == 4 && ctx.StringLiteral() != null) {
            return graphTraversal.as((String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()));
        }
        throw new UnsupportedEvalException(ctx.getClass(), "supported pattern is [as('..')]");
    }

    @Override
    public Traversal visitTraversalMethod_group(GremlinGSParser.TraversalMethod_groupContext ctx) {
        Traversal traversal = graphTraversal.group();
        if (ctx.traversalMethod_group_keyby() != null) {
            traversal = visitTraversalMethod_group_keyby(ctx.traversalMethod_group_keyby());
        }
        if (ctx.traversalMethod_group_valueby() != null) {
            traversal = visitTraversalMethod_group_valueby(ctx.traversalMethod_group_valueby());
        }
        return traversal;
    }

    @Override
    public Traversal visitTraversalMethod_groupCount(
            GremlinGSParser.TraversalMethod_groupCountContext ctx) {
        Traversal traversal = graphTraversal.groupCount();
        if (ctx.traversalMethod_group_keyby() != null) {
            traversal = visitTraversalMethod_group_keyby(ctx.traversalMethod_group_keyby());
        }
        return traversal;
    }

    @Override
    public Traversal visitTraversalMethod_group_keyby(
            GremlinGSParser.TraversalMethod_group_keybyContext ctx) {
        int childCount = ctx.getChildCount();
        if (childCount == 3) {
            return graphTraversal.by();
        } else if (childCount == 4 && ctx.StringLiteral() != null) {
            return graphTraversal.by((String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()));
        } else if (childCount == 4 && ctx.nonStringKeyByList() != null) {
            GremlinGSParser.NonStringKeyByListContext nonStringCtxs = ctx.nonStringKeyByList();
            List<Traversal.Admin> keyByList = new ArrayList<>();
            for (int i = 0; i < nonStringCtxs.getChildCount(); ++i) {
                GremlinGSParser.NonStringKeyByContext keyByCtx = nonStringCtxs.nonStringKeyBy(i);
                if (keyByCtx == null) continue;
                keyByList.add(visitNestedTraversal(keyByCtx.nestedTraversal()).asAdmin());
            }
            ((IrCustomizedTraversal) graphTraversal).by(keyByList);
            return graphTraversal;
        } else {
            throw new UnsupportedEvalException(
                    ctx.getClass(),
                    "supported patterns are [group().by()]"
                            + " or [group().by('name')] or group().by(non_string_key_list)");
        }
    }

    @Override
    public Traversal visitTraversalMethod_group_valueby(
            GremlinGSParser.TraversalMethod_group_valuebyContext ctx) {
        int childCount = ctx.getChildCount();
        if (childCount == 3) {
            return graphTraversal.by();
        } else if (childCount == 4 && ctx.StringLiteral() != null) {
            return graphTraversal.by((String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()));
        } else if (childCount == 4 && ctx.nonStringValueByList() != null) {
            GremlinGSParser.NonStringValueByListContext nonStringCtxs = ctx.nonStringValueByList();
            List<Traversal.Admin> valueByList = new ArrayList<>();
            for (int i = 0; i < nonStringCtxs.getChildCount(); ++i) {
                GremlinGSParser.NonStringValueByContext valueByCtx =
                        nonStringCtxs.nonStringValueBy(i);
                if (valueByCtx == null) continue;
                valueByList.add(getValueByAsNestedTraversal(valueByCtx).asAdmin());
            }
            ((IrCustomizedTraversal) graphTraversal).by(valueByList);
            return graphTraversal;
        } else {
            throw new UnsupportedEvalException(
                    ctx.getClass(),
                    "supported patterns are [group().by(..).by()] or [group().by(..).by('name')] or"
                            + " group().by(..).by(non_string_value_list)");
        }
    }

    private Traversal getValueByAsNestedTraversal(GremlinGSParser.NonStringValueByContext ctx) {
        TraversalMethodVisitor nestedVisitor =
                new TraversalMethodVisitor(
                        gvisitor, GremlinAntlrToJava.getTraversalSupplier().get());
        Traversal nestedTraversal = null;
        if (ctx.traversalMethod_dedup() != null) {
            nestedTraversal = nestedVisitor.visitTraversalMethod_dedup(ctx.traversalMethod_dedup());
            if (ctx.oC_AggregateFunctionInvocation() != null) {
                String functionName = ctx.oC_AggregateFunctionInvocation().getChild(0).getText();
                if (functionName.equals("count") || functionName.equals("fold")) {
                    nestedTraversal =
                            nestedVisitor.visitOC_AggregateFunctionInvocation(
                                    ctx.oC_AggregateFunctionInvocation());
                }
            }
        } else if (ctx.oC_AggregateFunctionInvocation() != null) {
            if (ctx.traversalMethod_select() != null) {
                nestedVisitor.visitTraversalMethod_select(ctx.traversalMethod_select());
            }
            if (ctx.traversalMethod_values() != null) {
                nestedVisitor.visitTraversalMethod_values(ctx.traversalMethod_values());
            }
            nestedTraversal =
                    nestedVisitor.visitOC_AggregateFunctionInvocation(
                            ctx.oC_AggregateFunctionInvocation());
        }
        if (ctx.traversalMethod_as() != null) {
            nestedTraversal = nestedVisitor.visitTraversalMethod_as(ctx.traversalMethod_as());
        }
        if (nestedTraversal == null) {
            throw new UnsupportedEvalException(ctx.getClass(), "aggregate function should exist");
        }
        return nestedTraversal;
    }

    @Override
    public Traversal visitOC_AggregateFunctionInvocation(
            GremlinGSParser.OC_AggregateFunctionInvocationContext ctx) {
        String functionName = ctx.getChild(0).getText();
        switch (functionName) {
            case "count":
                return graphTraversal.count();
            case "sum":
                return graphTraversal.sum();
            case "min":
                return graphTraversal.min();
            case "max":
                return graphTraversal.max();
            case "mean":
                return graphTraversal.mean();
            case "fold":
                return graphTraversal.fold();
            default:
                throw new UnsupportedEvalException(
                        ctx.getClass(),
                        "supported aggregation functions are count/sum/min/max/mean/fold");
        }
    }

    @Override
    public Traversal visitTraversalMethod_values(
            GremlinGSParser.TraversalMethod_valuesContext ctx) {
        if (ctx.getChildCount() == 4 && ctx.StringLiteral() != null) {
            return graphTraversal.values(
                    (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()));
        }
        throw new UnsupportedEvalException(ctx.getClass(), "supported pattern is [values('..')]");
    }

    @Override
    public Traversal visitTraversalMethod_dedup(GremlinGSParser.TraversalMethod_dedupContext ctx) {
        List<String> dedupTags =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        graphTraversal.dedup(dedupTags.toArray(new String[0]));
        DedupGlobalStep dedup = (DedupGlobalStep) graphTraversal.asAdmin().getEndStep();
        if (ctx.traversalMethod_dedupby() != null) {
            GremlinGSParser.TraversalMethod_dedupbyContext byCtx = ctx.traversalMethod_dedupby();
            if (byCtx.StringLiteral() != null) {
                dedup.modulateBy((String) LiteralVisitor.INSTANCE.visit(byCtx.StringLiteral()));
            } else if (byCtx.traversalToken() != null) {
                dedup.modulateBy(
                        TraversalEnumParser.parseTraversalEnumFromContext(
                                T.class, byCtx.traversalToken()));
            } else if (byCtx.nestedTraversal() != null) {
                Traversal nested = visitNestedTraversal(byCtx.nestedTraversal());
                dedup.modulateBy(nested.asAdmin());
            }
        }
        return graphTraversal;
    }

    @Override
    public Traversal visitTraversalMethod_where(GremlinGSParser.TraversalMethod_whereContext ctx) {
        if (ctx.StringLiteral() != null && ctx.traversalPredicate() != null) {
            graphTraversal.where(
                    (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()),
                    TraversalPredicateVisitor.getInstance()
                            .visitTraversalPredicate(ctx.traversalPredicate()));
        } else if (ctx.traversalPredicate() != null) {
            graphTraversal.where(
                    TraversalPredicateVisitor.getInstance()
                            .visitTraversalPredicate(ctx.traversalPredicate()));
        } else if (ctx.traversalMethod_not() != null) {
            visitTraversalMethod_not(ctx.traversalMethod_not());
        } else if (ctx.traversalMethod_expr() != null) { // where(expr(...))
            visitExpr(ctx.traversalMethod_expr(), ExprStep.Type.FILTER);
        } else if (ctx.nestedTraversal() != null) {
            Traversal whereTraversal = visitNestedTraversal(ctx.nestedTraversal());
            graphTraversal.where(whereTraversal);
        } else {
            throw new UnsupportedEvalException(
                    ctx.getClass(),
                    "supported pattern is [where(P.eq(...))] or [where(a, P.eq(...))] or"
                            + " [where(sub_traversal)]");
        }
        if (ctx.traversalMethod_whereby_list() != null) {
            ByModulating byModulating = (ByModulating) graphTraversal.asAdmin().getEndStep();
            int childCount = ctx.traversalMethod_whereby_list().getChildCount();
            for (int i = 0; i < childCount; ++i) {
                GremlinGSParser.TraversalMethod_wherebyContext byCtx =
                        ctx.traversalMethod_whereby_list().traversalMethod_whereby(i);
                if (byCtx == null) continue;
                int byChildCount = byCtx.getChildCount();
                if (byChildCount == 3) { // by()
                    byModulating.modulateBy();
                } else if (byChildCount == 4 && byCtx.StringLiteral() != null) {
                    byModulating.modulateBy(
                            (String) LiteralVisitor.INSTANCE.visit(byCtx.StringLiteral()));
                } else if (byCtx.traversalMethod_values() != null) {
                    // select(..).by(valueMap())
                    TraversalMethodVisitor nestedVisitor =
                            new TraversalMethodVisitor(
                                    gvisitor, GremlinAntlrToJava.getTraversalSupplier().get());
                    Traversal nestedTraversal =
                            nestedVisitor.visitTraversalMethod_values(
                                    byCtx.traversalMethod_values());
                    byModulating.modulateBy(nestedTraversal.asAdmin());
                } else if (byChildCount == 4 && byCtx.nestedTraversal() != null) {
                    Traversal byTraversal = visitNestedTraversal(byCtx.nestedTraversal());
                    byModulating.modulateBy(byTraversal.asAdmin());
                }
            }
        }
        return graphTraversal;
    }

    @Override
    public Traversal visitTraversalMethod_not(GremlinGSParser.TraversalMethod_notContext ctx) {
        if (ctx.nestedTraversal() != null) {
            Traversal whereTraversal = visitNestedTraversal(ctx.nestedTraversal());
            return graphTraversal.not(whereTraversal);
        } else {
            throw new UnsupportedEvalException(
                    ctx.getClass(), "supported pattern is [not(..out()..)]");
        }
    }

    @Override
    public Traversal visitTraversalMethod_union(GremlinGSParser.TraversalMethod_unionContext ctx) {
        if (ctx.nestedTraversalExpr() != null) {
            Traversal[] unionTraversals =
                    (new NestedTraversalSourceListVisitor((GremlinGSBaseVisitor) this))
                            .visitNestedTraversalExpr(ctx.nestedTraversalExpr());
            return graphTraversal.union(unionTraversals);
        } else {
            throw new UnsupportedEvalException(
                    ctx.getClass(), "supported pattern is [union(__.out(), ...)]");
        }
    }

    @Override
    public Traversal visitTraversalMethod_identity(
            final GremlinGSParser.TraversalMethod_identityContext ctx) {
        return graphTraversal.identity();
    }

    @Override
    public Traversal visitTraversalMethod_match(GremlinGSParser.TraversalMethod_matchContext ctx) {
        if (ctx.nestedTraversalExpr() != null) {
            Traversal[] matchTraversals =
                    (new NestedTraversalSourceListVisitor((GremlinGSBaseVisitor) this))
                            .visitNestedTraversalExpr(ctx.nestedTraversalExpr());
            return graphTraversal.match(matchTraversals);
        } else {
            throw new UnsupportedEvalException(
                    ctx.getClass(), "supported pattern is [match(as('a').out()..., ...)]");
        }
    }

    @Override
    public Traversal visitTraversalMethod_subgraph(
            final GremlinGSParser.TraversalMethod_subgraphContext ctx) {
        Step endStep = graphTraversal.asAdmin().getEndStep();
        if (!isEdgeOutputStep(endStep)) {
            throw new InvalidGremlinScriptException(
                    "edge induced subgraph should follow an edge output operator [E, inE, outE,"
                            + " bothE]");
        }
        return graphTraversal.subgraph((String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()));
    }

    // steps which have edge type as output, i.e. E(), inE(), outE(), bothE()
    private boolean isEdgeOutputStep(Step step) {
        if (step == null) return false;
        return step instanceof VertexStep
                || step instanceof GraphStep && ((GraphStep) step).returnsEdge();
    }

    @Override
    public Traversal visitTraversalMethod_bothV(
            final GremlinGSParser.TraversalMethod_bothVContext ctx) {
        return graphTraversal.bothV();
    }

    @Override
    public Traversal visitTraversalMethod_unfold(
            final GremlinGSParser.TraversalMethod_unfoldContext ctx) {
        return graphTraversal.unfold();
    }

    @Override
    public Traversal visitTraversalMethod_coin(
            final GremlinGSParser.TraversalMethod_coinContext ctx) {
        return graphTraversal.coin(
                ((Number) LiteralVisitor.INSTANCE.visit(ctx.oC_DoubleLiteral())).doubleValue());
    }

    @Override
    public Traversal visitTraversalMethod_sample(
            final GremlinGSParser.TraversalMethod_sampleContext ctx) {
        Number amountToSample = (Number) LiteralVisitor.INSTANCE.visit(ctx.oC_IntegerLiteral());
        graphTraversal.sample(amountToSample.intValue());
        SampleGlobalStep sample = (SampleGlobalStep) graphTraversal.asAdmin().getEndStep();
        if (ctx.traversalMethod_sampleby() != null) {
            GremlinGSParser.TraversalMethod_samplebyContext byCtx = ctx.traversalMethod_sampleby();
            if (byCtx.traversalToken() != null) {
                sample.modulateBy(
                        TraversalEnumParser.parseTraversalEnumFromContext(
                                T.class, byCtx.traversalToken()));
            } else if (byCtx.StringLiteral() != null) {
                sample.modulateBy((String) LiteralVisitor.INSTANCE.visit(byCtx.StringLiteral()));
            } else if (byCtx.nestedTraversal() != null) {
                Traversal nested = visitNestedTraversal(byCtx.nestedTraversal());
                sample.modulateBy(nested.asAdmin());
            }
        }
        return graphTraversal;
    }

    @Override
    public Traversal visitTraversalMethod_with(GremlinGSParser.TraversalMethod_withContext ctx) {
        Step endStep = graphTraversal.asAdmin().getEndStep();
        if (!(endStep instanceof PathExpandStep)) {
            throw new UnsupportedEvalException(
                    ctx.getClass(),
                    "with should follow source or path expand, i.e. g.with(..) or"
                            + " out('1..2').with(..)");
        }
        String optKey = (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral());
        Object optValue = LiteralVisitor.INSTANCE.visit(ctx.oC_Literal());
        return graphTraversal.with(optKey, optValue);
    }

    @Override
    public Traversal visitTraversalMethod_id(GremlinGSParser.TraversalMethod_idContext ctx) {
        return graphTraversal.id();
    }

    @Override
    public Traversal visitTraversalMethod_label(GremlinGSParser.TraversalMethod_labelContext ctx) {
        return graphTraversal.label();
    }

    @Override
    public Traversal visitTraversalMethod_constant(
            GremlinGSParser.TraversalMethod_constantContext ctx) {
        return graphTraversal.constant(LiteralVisitor.INSTANCE.visit(ctx.oC_Literal()));
    }

    private Traversal visitExpr(
            GremlinGSParser.TraversalMethod_exprContext ctx, ExprStep.Type type) {
        if (ctx.StringLiteral() != null) {
            IrCustomizedTraversal traversal = (IrCustomizedTraversal) graphTraversal;
            return traversal.expr(
                    (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()), type);
        } else {
            throw new UnsupportedEvalException(ctx.getClass(), "supported pattern is [expr(...)]");
        }
    }
}
