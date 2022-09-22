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

import com.alibaba.graphscope.gremlin.Utils;
import com.alibaba.graphscope.gremlin.exception.UnsupportedEvalException;
import com.alibaba.graphscope.gremlin.plugin.step.ExprStep;
import com.alibaba.graphscope.gremlin.plugin.step.PathExpandStep;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversal;

import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSBaseVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSParser;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
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
        if (ctx.getChildCount() == 4) {
            return graphTraversal.hasLabel(
                    GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
        } else if (ctx.stringLiteralList() != null) {
            return graphTraversal.hasLabel(
                    GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        } else {
            throw new UnsupportedEvalException(
                    ctx.getClass(),
                    "supported pattern is [hasLabel('str')] or hasLabel('str1', ...)");
        }
    }

    @Override
    public Traversal visitTraversalMethod_hasId(GremlinGSParser.TraversalMethod_hasIdContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.hasId(
                    GenericLiteralVisitor.getInstance().visitIntegerLiteral(ctx.integerLiteral()));
        } else if (ctx.integerLiteral() != null && ctx.integerLiteralList() != null) {
            return graphTraversal.hasId(
                    GenericLiteralVisitor.getInstance().visitIntegerLiteral(ctx.integerLiteral()),
                    GenericLiteralVisitor.getIntegerLiteralList(ctx.integerLiteralList()));
        } else {
            throw new UnsupportedEvalException(
                    ctx.getClass(), "supported pattern is [hasId(1)] or hasId(1, 2, ...)");
        }
    }

    @Override
    public Traversal visitTraversalMethod_has(GremlinGSParser.TraversalMethod_hasContext ctx) {
        String notice =
                "supported pattern is [has('key', 'value')] or [has('key', P)] or [has('label',"
                        + " 'key', 'value')] or [has('label', 'key', P)]";
        int childCount = ctx.getChildCount();
        if (childCount == 6 && ctx.genericLiteral() != null) {
            return graphTraversal.has(
                    GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0)),
                    GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()));
        } else if (childCount == 6 && ctx.traversalPredicate() != null) {
            return graphTraversal.has(
                    GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0)),
                    TraversalPredicateVisitor.getInstance()
                            .visitTraversalPredicate(ctx.traversalPredicate()));
        } else if (childCount == 8 && ctx.genericLiteral() != null) {
            return graphTraversal.has(
                    GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0)),
                    GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(1)),
                    GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()));
        } else if (childCount == 8 && ctx.traversalPredicate() != null) {
            return graphTraversal.has(
                    GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0)),
                    GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(1)),
                    TraversalPredicateVisitor.getInstance()
                            .visitTraversalPredicate(ctx.traversalPredicate()));
        } else if (childCount == 4 && ctx.stringLiteral() != null) {
            P eqAny = P.eq(AnyValue.INSTANCE);
            return graphTraversal.has(
                    GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0)), eqAny);
        } else {
            throw new UnsupportedEvalException(ctx.getClass(), notice);
        }
    }

    @Override
    public Traversal visitTraversalMethod_hasNot(
            GremlinGSParser.TraversalMethod_hasNotContext ctx) {
        P neqAny = P.neq(AnyValue.INSTANCE);
        return graphTraversal.has(
                GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()), neqAny);
    }

    @Override
    public Traversal visitTraversalMethod_is(GremlinGSParser.TraversalMethod_isContext ctx) {
        if (ctx.genericLiteral() != null) {
            return graphTraversal.is(
                    GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()));
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
        String[] labels = GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList());
        if (labels != null && labels.length > 0 && isRangeExpression(labels[0])) {
            GraphTraversal nestedRange = GremlinAntlrToJava.getTraversalSupplier().get();
            String[] ranges = labels[0].split("\\.\\.");
            RangeGlobalStep rangeStep =
                    new RangeGlobalStep(
                            nestedRange.asAdmin(),
                            Integer.valueOf(ranges[0]),
                            Integer.valueOf(ranges[1]));
            nestedRange.asAdmin().addStep(rangeStep);
            IrCustomizedTraversal traversal = (IrCustomizedTraversal) graphTraversal;
            labels = Utils.removeStringEle(0, labels);
            return traversal.out(nestedRange, labels);
        } else {
            return graphTraversal.out(labels);
        }
    }

    private boolean isRangeExpression(String label) {
        return label.matches("^\\d+\\.\\.\\d+");
    }

    @Override
    public Traversal visitTraversalMethod_in(GremlinGSParser.TraversalMethod_inContext ctx) {
        String[] labels = GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList());
        if (labels != null && labels.length > 0 && isRangeExpression(labels[0])) {
            GraphTraversal nestedRange = GremlinAntlrToJava.getTraversalSupplier().get();
            String[] ranges = labels[0].split("\\.\\.");
            RangeGlobalStep rangeStep =
                    new RangeGlobalStep(
                            nestedRange.asAdmin(),
                            Integer.valueOf(ranges[0]),
                            Integer.valueOf(ranges[1]));
            nestedRange.asAdmin().addStep(rangeStep);
            IrCustomizedTraversal traversal = (IrCustomizedTraversal) graphTraversal;
            labels = Utils.removeStringEle(0, labels);
            return traversal.in(nestedRange, labels);
        } else {
            return graphTraversal.in(labels);
        }
    }

    @Override
    public Traversal visitTraversalMethod_both(GremlinGSParser.TraversalMethod_bothContext ctx) {
        String[] labels = GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList());
        if (labels != null && labels.length > 0 && isRangeExpression(labels[0])) {
            GraphTraversal nestedRange = GremlinAntlrToJava.getTraversalSupplier().get();
            String[] ranges = labels[0].split("\\.\\.");
            RangeGlobalStep rangeStep =
                    new RangeGlobalStep(
                            nestedRange.asAdmin(),
                            Integer.valueOf(ranges[0]),
                            Integer.valueOf(ranges[1]));
            nestedRange.asAdmin().addStep(rangeStep);
            IrCustomizedTraversal traversal = (IrCustomizedTraversal) graphTraversal;
            labels = Utils.removeStringEle(0, labels);
            return traversal.both(nestedRange, labels);
        } else {
            return graphTraversal.both(labels);
        }
    }

    @Override
    public Traversal visitTraversalMethod_outE(GremlinGSParser.TraversalMethod_outEContext ctx) {
        String[] labels = GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList());
        if (ctx.traversalMethod_inV() != null) {
            return graphTraversal.out(labels);
        } else {
            return graphTraversal.outE(labels);
        }
    }

    @Override
    public Traversal visitTraversalMethod_inE(GremlinGSParser.TraversalMethod_inEContext ctx) {
        String[] labels = GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList());
        if (ctx.traversalMethod_outV() != null) {
            return graphTraversal.in(labels);
        } else {
            return graphTraversal.inE(labels);
        }
    }

    @Override
    public Traversal visitTraversalMethod_bothE(GremlinGSParser.TraversalMethod_bothEContext ctx) {
        String[] labels = GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList());
        if (ctx.traversalMethod_otherV() != null) {
            return graphTraversal.both(labels);
        } else {
            return graphTraversal.bothE(labels);
        }
    }

    @Override
    public Traversal visitTraversalMethod_limit(GremlinGSParser.TraversalMethod_limitContext ctx) {
        Number integer =
                (Number)
                        GenericLiteralVisitor.getInstance()
                                .visitIntegerLiteral(ctx.integerLiteral());
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
        return graphTraversal.valueMap(
                GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    @Override
    public Traversal visitTraversalMethod_select(
            GremlinGSParser.TraversalMethod_selectContext ctx) {
        if (ctx.stringLiteral() != null) {
            String tag = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral());
            // set tags
            if (ctx.stringLiteralList() != null) {
                String[] tags = GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList());
                if (tags.length == 0) { // select one tag
                    graphTraversal.select(tag);
                } else if (tags.length == 1) {
                    graphTraversal.select(tag, tags[0]);
                } else {
                    String[] otherTags = Utils.removeStringEle(0, tags);
                    graphTraversal.select(tag, tags[0], otherTags);
                }
            } else { // select one tag
                graphTraversal.select(tag);
            }
        } else if (ctx.traversalColumn() != null) {
            Column column =
                    TraversalEnumParser.parseTraversalEnumFromContext(
                            Column.class, ctx.traversalColumn());
            graphTraversal.select(column);
        } else if (ctx.traversalMethod_expr() != null) {
            visitExpr(ctx.traversalMethod_expr(), ExprStep.Type.PROJECTION);
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
                        && byCtx.stringLiteral() != null) { // select(..).by('name')
                    step.modulateBy(GenericLiteralVisitor.getStringLiteral(byCtx.stringLiteral()));
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
                if (byCtx.stringLiteral() != null) {
                    strAsKey = GenericLiteralVisitor.getStringLiteral(byCtx.stringLiteral());
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
        if (ctx.getChildCount() == 4 && ctx.stringLiteral() != null) {
            return graphTraversal.as(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
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
        } else if (childCount == 4 && ctx.stringLiteral() != null) {
            return graphTraversal.by(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
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
        } else if (childCount == 4 && ctx.stringLiteral() != null) {
            return graphTraversal.by(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
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
            if (ctx.traversalMethod_count() != null) {
                nestedTraversal =
                        nestedVisitor.visitTraversalMethod_count(ctx.traversalMethod_count());
            } else if (ctx.traversalMethod_fold() != null) {
                nestedTraversal =
                        nestedVisitor.visitTraversalMethod_fold(ctx.traversalMethod_fold());
            }
        } else if (ctx.traversalMethod_aggregate_func() != null) {
            if (ctx.traversalMethod_select() != null) {
                nestedVisitor.visitTraversalMethod_select(ctx.traversalMethod_select());
            }
            if (ctx.traversalMethod_values() != null) {
                nestedVisitor.visitTraversalMethod_values(ctx.traversalMethod_values());
            }
            nestedTraversal =
                    nestedVisitor.visitTraversalMethod_aggregate_func(
                            ctx.traversalMethod_aggregate_func());
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
    public Traversal visitTraversalMethod_aggregate_func(
            GremlinGSParser.TraversalMethod_aggregate_funcContext ctx) {
        if (ctx.traversalMethod_count() != null) {
            return visitTraversalMethod_count(ctx.traversalMethod_count());
        } else if (ctx.traversalMethod_fold() != null) {
            return visitTraversalMethod_fold(ctx.traversalMethod_fold());
        } else if (ctx.traversalMethod_sum() != null) {
            return visitTraversalMethod_sum(ctx.traversalMethod_sum());
        } else if (ctx.traversalMethod_min() != null) {
            return visitTraversalMethod_min(ctx.traversalMethod_min());
        } else if (ctx.traversalMethod_max() != null) {
            return visitTraversalMethod_max(ctx.traversalMethod_max());
        } else if (ctx.traversalMethod_mean() != null) {
            return visitTraversalMethod_mean(ctx.traversalMethod_mean());
        } else {
            throw new UnsupportedEvalException(
                    ctx.getClass(),
                    "supported aggregation functions are count/sum/min/max/mean/fold");
        }
    }

    @Override
    public Traversal visitTraversalMethod_sum(GremlinGSParser.TraversalMethod_sumContext ctx) {
        return graphTraversal.sum();
    }

    @Override
    public Traversal visitTraversalMethod_min(GremlinGSParser.TraversalMethod_minContext ctx) {
        return graphTraversal.min();
    }

    @Override
    public Traversal visitTraversalMethod_max(GremlinGSParser.TraversalMethod_maxContext ctx) {
        return graphTraversal.max();
    }

    @Override
    public Traversal visitTraversalMethod_mean(GremlinGSParser.TraversalMethod_meanContext ctx) {
        return graphTraversal.mean();
    }

    @Override
    public Traversal visitTraversalMethod_count(GremlinGSParser.TraversalMethod_countContext ctx) {
        return graphTraversal.count();
    }

    @Override
    public Traversal visitTraversalMethod_values(
            GremlinGSParser.TraversalMethod_valuesContext ctx) {
        if (ctx.getChildCount() == 4 && ctx.stringLiteral() != null) {
            return graphTraversal.values(
                    GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
        }
        throw new UnsupportedEvalException(ctx.getClass(), "supported pattern is [values('..')]");
    }

    @Override
    public Traversal visitTraversalMethod_fold(GremlinGSParser.TraversalMethod_foldContext ctx) {
        return graphTraversal.fold();
    }

    @Override
    public Traversal visitTraversalMethod_dedup(GremlinGSParser.TraversalMethod_dedupContext ctx) {
        graphTraversal.dedup(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        DedupGlobalStep dedup = (DedupGlobalStep) graphTraversal.asAdmin().getEndStep();
        if (ctx.traversalMethod_dedupby() != null) {
            GremlinGSParser.TraversalMethod_dedupbyContext byCtx = ctx.traversalMethod_dedupby();
            if (byCtx.stringLiteral() != null) {
                dedup.modulateBy(GenericLiteralVisitor.getStringLiteral(byCtx.stringLiteral()));
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
        if (ctx.stringLiteral() != null && ctx.traversalPredicate() != null) {
            graphTraversal.where(
                    GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
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
                } else if (byChildCount == 4 && byCtx.stringLiteral() != null) {
                    byModulating.modulateBy(
                            GenericLiteralVisitor.getStringLiteral(byCtx.stringLiteral()));
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
        return graphTraversal.subgraph(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    @Override
    public Traversal visitTraversalMethod_bothV(
            final GremlinGSParser.TraversalMethod_bothVContext ctx) {
        return graphTraversal.bothV();
    }

    @Override
    public Traversal visitTraversalMethod_coin(
            final GremlinGSParser.TraversalMethod_coinContext ctx) {
        Step endStep = graphTraversal.asAdmin().getEndStep();
        if (!(endStep instanceof GraphStep)) {
            throw new UnsupportedEvalException(
                    ctx.getClass(), "coin should follow source step, i.e. V().coin(0.2)");
        }
        return graphTraversal.coin(Double.valueOf(ctx.floatLiteral().getText()));
    }

    @Override
    public Traversal visitTraversalMethod_with(GremlinGSParser.TraversalMethod_withContext ctx) {
        Step endStep = graphTraversal.asAdmin().getEndStep();
        if (!(endStep instanceof PathExpandStep)) {
            throw new UnsupportedEvalException(
                    ctx.getClass(), "with should follow path expand, i.e. out('1..2').with(..)");
        }
        String optKey = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0));
        String optValue = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(1));
        return graphTraversal.with(optKey, optValue);
    }

    private Traversal visitExpr(
            GremlinGSParser.TraversalMethod_exprContext ctx, ExprStep.Type type) {
        if (ctx.stringLiteral() != null) {
            IrCustomizedTraversal traversal = (IrCustomizedTraversal) graphTraversal;
            return traversal.expr(
                    GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()), type);
        } else {
            throw new UnsupportedEvalException(ctx.getClass(), "supported pattern is [expr(...)]");
        }
    }
}
