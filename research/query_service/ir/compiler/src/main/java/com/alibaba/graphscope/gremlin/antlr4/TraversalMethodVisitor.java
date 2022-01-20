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
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSBaseVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSParser;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;

public class TraversalMethodVisitor extends TraversalRootVisitor<GraphTraversal> {
    final GraphTraversal graphTraversal;

    public TraversalMethodVisitor(final GremlinGSBaseVisitor<GraphTraversalSource> gvisitor,
                                  final GraphTraversal graphTraversal) {
        super(gvisitor);
        this.graphTraversal = graphTraversal;
    }

    @Override
    public Traversal visitTraversalMethod(GremlinGSParser.TraversalMethodContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Traversal visitTraversalMethod_hasLabel(GremlinGSParser.TraversalMethod_hasLabelContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.hasLabel(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
        } else if (ctx.stringLiteralList() != null) {
            return graphTraversal.hasLabel(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        } else {
            throw new UnsupportedEvalException(ctx.getClass(),
                    "supported pattern is [hasLabel('str')] or hasLabel('str1', ...)");
        }
    }

    @Override
    public Traversal visitTraversalMethod_hasId(GremlinGSParser.TraversalMethod_hasIdContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.hasId(GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()));
        } else if (ctx.genericLiteralList() != null) {
            return graphTraversal.hasId(GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()),
                    GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList()));
        } else {
            throw new UnsupportedEvalException(ctx.getClass(),
                    "supported pattern is [hasId(1)] or hasId(1, 2, ...)");
        }
    }

    @Override
    public Traversal visitTraversalMethod_has(GremlinGSParser.TraversalMethod_hasContext ctx) {
        String notice = "supported pattern is " +
                "[has('key', 'value')] or [has('key', P)] or [has('label', 'key', 'value')] or [has('label', 'key', P)]";
        int childCount = ctx.getChildCount();
        if (childCount == 6 && ctx.genericLiteral() != null) {
            return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0)),
                    GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()));
        } else if (childCount == 6 && ctx.traversalPredicate() != null) {
            return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0)),
                    TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
        } else if (childCount == 8 && ctx.genericLiteral() != null) {
            return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0)),
                    GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(1)),
                    GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()));
        } else if (childCount == 8 && ctx.traversalPredicate() != null) {
            return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0)),
                    GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(1)),
                    TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
        } else {
            throw new UnsupportedEvalException(ctx.getClass(), notice);
        }
    }

    @Override
    public Traversal visitTraversalMethod_is(GremlinGSParser.TraversalMethod_isContext ctx) {
        if (ctx.genericLiteral() != null) {
            return graphTraversal.is(GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()));
        } else if (ctx.traversalPredicate() != null) {
            return graphTraversal.is(
                    TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
        } else {
            throw new UnsupportedEvalException(ctx.getClass(), "supported pattern is [is(27)] or [is(eq(27))])");
        }
    }

    @Override
    public Traversal visitTraversalMethod_out(GremlinGSParser.TraversalMethod_outContext ctx) {
        return graphTraversal.out(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    @Override
    public Traversal visitTraversalMethod_in(GremlinGSParser.TraversalMethod_inContext ctx) {
        return graphTraversal.in(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    @Override
    public Traversal visitTraversalMethod_both(GremlinGSParser.TraversalMethod_bothContext ctx) {
        return graphTraversal.both(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

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
        return graphTraversal.limit((Integer) GenericLiteralVisitor.getInstance().visitIntegerLiteral(ctx.integerLiteral()));
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
    public Traversal visitTraversalMethod_otherV(GremlinGSParser.TraversalMethod_otherVContext ctx) {
        return graphTraversal.otherV();
    }

    @Override
    public Traversal visitTraversalMethod_valueMap(GremlinGSParser.TraversalMethod_valueMapContext ctx) {
        return graphTraversal.valueMap(GenericLiteralVisitor.getstringLiteralExpr(ctx.stringLiteralExpr()));
    }

    @Override
    public Traversal visitTraversalMethod_select(GremlinGSParser.TraversalMethod_selectContext ctx) {
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
        // set by traversal
        if (ctx.traversalMethod_selectby_list() != null) {
            ByModulating step = (ByModulating) graphTraversal.asAdmin().getEndStep();
            int childCount = ctx.traversalMethod_selectby_list().getChildCount();
            for (int i = 0; i < childCount; ++i) {
                GremlinGSParser.TraversalMethod_selectbyContext byCtx =
                        ctx.traversalMethod_selectby_list().traversalMethod_selectby(i);
                if (byCtx == null) continue;
                // select(..).by('name')
                if (byCtx.stringLiteral() != null) {
                    step.modulateBy(GenericLiteralVisitor.getStringLiteral(byCtx.stringLiteral()));
                } else if (byCtx.traversalMethod_valueMap() != null) {
                    // select(..).by(valueMap())
                    TraversalMethodVisitor nestedVisitor = new TraversalMethodVisitor(gvisitor,
                            GremlinAntlrToJava.getTraversalSupplier().get());
                    Traversal nestedTraversal = nestedVisitor.visitTraversalMethod_valueMap(byCtx.traversalMethod_valueMap());
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
                GremlinGSParser.TraversalMethod_orderbyContext byCtx = ctx.traversalMethod_orderby_list().traversalMethod_orderby(i);
                if (byCtx == null) continue;
                if (byCtx.getChildCount() == 4 && byCtx.traversalOrder() != null) {
                    Order order = TraversalEnumParser.parseTraversalEnumFromContext(Order.class, byCtx.traversalOrder());
                    step.modulateBy(order);
                } else if (byCtx.getChildCount() == 6 && byCtx.traversalOrder() != null && byCtx.stringLiteral() != null) {
                    Order order = TraversalEnumParser.parseTraversalEnumFromContext(Order.class, byCtx.traversalOrder());
                    step.modulateBy(GenericLiteralVisitor.getStringLiteral(byCtx.stringLiteral()), order);
                } else {
                    throw new UnsupportedEvalException(byCtx.getClass(),
                            "supported pattern is [order().by(order)] or [order().by('key', order)]");
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
    public Traversal visitTraversalMethod_groupCount(GremlinGSParser.TraversalMethod_groupCountContext ctx) {
        Traversal traversal = graphTraversal.groupCount();
        if (ctx.traversalMethod_group_keyby() != null) {
            traversal = visitTraversalMethod_group_keyby(ctx.traversalMethod_group_keyby());
        }
        return traversal;
    }

    @Override
    public Traversal visitTraversalMethod_group_keyby(GremlinGSParser.TraversalMethod_group_keybyContext ctx) {
        int childCount = ctx.getChildCount();
        if (childCount == 4 && ctx.stringLiteral() != null) {
            return graphTraversal.by(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
        } else if (childCount == 4 && ctx.traversalMethod_values() != null) {
            TraversalMethodVisitor nestedVisitor = new TraversalMethodVisitor(gvisitor,
                    GremlinAntlrToJava.getTraversalSupplier().get());
            Traversal nestedTraversal = nestedVisitor.visitTraversalMethod_values(ctx.traversalMethod_values());
            return graphTraversal.by(nestedTraversal);
        } else if (childCount >= 6 && ctx.traversalMethod_values() != null) {
            TraversalMethodVisitor nestedVisitor = new TraversalMethodVisitor(gvisitor,
                    GremlinAntlrToJava.getTraversalSupplier().get());
            Traversal nestedTraversal = nestedVisitor.visitTraversalMethod_values(ctx.traversalMethod_values());
            if (ctx.traversalMethod_as() != null) {
                nestedTraversal = nestedVisitor.visitTraversalMethod_as(ctx.traversalMethod_as());
            }
            return graphTraversal.by(nestedTraversal);
        } else {
            throw new UnsupportedEvalException(ctx.getClass(),
                    "supported pattern is [group().by('..')] or [group().by(values('..'))] or [group().by(values('..')).as('..')]");
        }
    }

    @Override
    public Traversal visitTraversalMethod_group_valueby(GremlinGSParser.TraversalMethod_group_valuebyContext ctx) {
        int childCount = ctx.getChildCount();
        if (childCount == 4 && ctx.traversalMethod_aggregate_func() != null) {
            TraversalMethodVisitor nestedVisitor = new TraversalMethodVisitor(gvisitor,
                    GremlinAntlrToJava.getTraversalSupplier().get());
            Traversal nestedTraversal = nestedVisitor.visitTraversalMethod_aggregate_func(ctx.traversalMethod_aggregate_func());
            return graphTraversal.by(nestedTraversal);
        } else if (childCount >= 6 && ctx.traversalMethod_aggregate_func() != null) {
            TraversalMethodVisitor nestedVisitor = new TraversalMethodVisitor(gvisitor,
                    GremlinAntlrToJava.getTraversalSupplier().get());
            Traversal nestedTraversal = nestedVisitor.visitTraversalMethod_aggregate_func(ctx.traversalMethod_aggregate_func());
            if (ctx.traversalMethod_as() != null) {
                nestedTraversal = nestedVisitor.visitTraversalMethod_as(ctx.traversalMethod_as());
            }
            return graphTraversal.by(nestedTraversal);
        } else {
            throw new UnsupportedEvalException(ctx.getClass(),
                    "supported pattern is [group().by(..).by(count())] or [group().by(..).by(fold())]" +
                            " or [group().by(..).by(count().as('..'))] or [group().by(..).by(fold().as('..'))]");
        }
    }

    @Override
    public Traversal visitTraversalMethod_aggregate_func(GremlinGSParser.TraversalMethod_aggregate_funcContext ctx) {
        if (ctx.traversalMethod_count() != null) {
            return visitTraversalMethod_count(ctx.traversalMethod_count());
        } else if (ctx.traversalMethod_fold() != null) {
            return visitTraversalMethod_fold(ctx.traversalMethod_fold());
        } else {
            throw new UnsupportedEvalException(ctx.getClass(), "supported pattern is [count()] or [fold()]");
        }
    }

    @Override
    public Traversal visitTraversalMethod_count(GremlinGSParser.TraversalMethod_countContext ctx) {
        return graphTraversal.count();
    }

    @Override
    public Traversal visitTraversalMethod_values(GremlinGSParser.TraversalMethod_valuesContext ctx) {
        if (ctx.getChildCount() == 4 && ctx.stringLiteral() != null) {
            return graphTraversal.values(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
        }
        throw new UnsupportedEvalException(ctx.getClass(), "supported pattern is [values('..')]");
    }

    @Override
    public Traversal visitTraversalMethod_fold(GremlinGSParser.TraversalMethod_foldContext ctx) {
        return graphTraversal.fold();
    }

    @Override
    public Traversal visitTraversalMethod_dedup(GremlinGSParser.TraversalMethod_dedupContext ctx) {
        return graphTraversal.dedup();
    }

    @Override
    public Traversal visitTraversalMethod_where(GremlinGSParser.TraversalMethod_whereContext ctx) {
        if (ctx.stringLiteral() != null && ctx.traversalPredicate() != null) {
            graphTraversal.where(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
        } else if (ctx.traversalPredicate() != null) {
            graphTraversal.where(TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
        } else {
            throw new UnsupportedEvalException(ctx.getClass(), "supported pattern is [where(P.eq(...))] or [where(a, P.eq(...))]");
        }
        if (ctx.traversalMethod_whereby_list() != null) {
            ByModulating byModulating = (ByModulating) graphTraversal.asAdmin().getEndStep();
            int childCount = ctx.traversalMethod_whereby_list().getChildCount();
            for (int i = 0; i < childCount; ++i) {
                GremlinGSParser.TraversalMethod_wherebyContext byCtx =
                        ctx.traversalMethod_whereby_list().traversalMethod_whereby(i);
                if (byCtx == null) continue;
                if (byCtx.stringLiteral() != null) {
                    byModulating.modulateBy(GenericLiteralVisitor.getStringLiteral(byCtx.stringLiteral()));
                } else if (byCtx.traversalMethod_values() != null) {
                    // select(..).by(valueMap())
                    TraversalMethodVisitor nestedVisitor = new TraversalMethodVisitor(gvisitor,
                            GremlinAntlrToJava.getTraversalSupplier().get());
                    Traversal nestedTraversal = nestedVisitor.visitTraversalMethod_values(byCtx.traversalMethod_values());
                    byModulating.modulateBy(nestedTraversal.asAdmin());
                }
            }
        }
        return graphTraversal;
    }
}
