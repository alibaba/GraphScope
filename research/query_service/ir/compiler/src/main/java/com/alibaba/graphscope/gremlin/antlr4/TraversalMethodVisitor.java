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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;

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
        if (ctx.genericLiteral() != null) {
            return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()));
        } else if (ctx.traversalPredicate() != null) {
            return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
        } else {
            throw new UnsupportedEvalException(ctx.getClass(),
                    "supported pattern is [has('key', 'value')] or [has('key', P)]");
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
        GraphTraversal traversal = graphTraversal.outE(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        if (ctx.traversalMethod_inV() != null) {
            return visitTraversalMethod_inV(ctx.traversalMethod_inV());
        } else {
            return traversal;
        }
    }

    @Override
    public Traversal visitTraversalMethod_inE(GremlinGSParser.TraversalMethod_inEContext ctx) {
        GraphTraversal traversal = graphTraversal.inE(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        if (ctx.traversalMethod_outV() != null) {
            return graphTraversal.outV();
        } else {
            return traversal;
        }
    }

    @Override
    public Traversal visitTraversalMethod_bothE(GremlinGSParser.TraversalMethod_bothEContext ctx) {
        GraphTraversal traversal = graphTraversal.bothE(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        if (ctx.traversalMethod_otherV() != null) {
            return graphTraversal.otherV();
        } else {
            return traversal;
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
            if (tags.length == 0) {
                graphTraversal.select(tag, "");
            } else if (tags.length == 1) {
                graphTraversal.select(tag, tags[0]);
            } else {
                String[] otherTags = Utils.removeStringEle(0, tags);
                graphTraversal.select(tag, tags[0], otherTags);
            }
        } else {
            graphTraversal.select(tag, "");
        }
        // set by traversal
        if (ctx.traversalMethod_selectby_list() != null) {
            SelectStep step = (SelectStep) graphTraversal.asAdmin().getEndStep();
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
}
