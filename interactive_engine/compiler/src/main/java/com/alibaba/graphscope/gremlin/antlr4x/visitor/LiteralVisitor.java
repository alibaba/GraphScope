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

import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.google.common.base.Preconditions;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.text.StringEscapeUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.stream.Collectors;

/**
 * This class is mainly used to parse the literal structure from the expression syntax.
 * In addition, the original literal implementation (antlr->traversal) has also been unified here.
 */
public class LiteralVisitor extends GremlinGSBaseVisitor<Object> {
    public static final LiteralVisitor INSTANCE = new LiteralVisitor();

    private LiteralVisitor() {}

    @Override
    public Object visitOC_BooleanLiteral(GremlinGSParser.OC_BooleanLiteralContext ctx) {
        return Boolean.valueOf(ctx.getText());
    }

    @Override
    public Object visitOC_IntegerLiteral(GremlinGSParser.OC_IntegerLiteralContext ctx) {
        String integerLiteral = ctx.getText().toLowerCase();
        try {
            if (integerLiteral.endsWith("l")) {
                integerLiteral = integerLiteral.substring(0, integerLiteral.length() - 1);
                return Long.decode(integerLiteral);
            }
            // try to parse it as integer first
            return Integer.decode(integerLiteral);
        } catch (NumberFormatException ignoredExpection1) {
            try {
                // If range exceeds integer limit, try to parse it as long
                return Long.decode(integerLiteral);
            } catch (NumberFormatException ignoredExpection2) {
                // If range exceeds Long limit, parse it as BigInteger
                // as the literal range is longer than long, the number of character should be much
                // more than 3,
                // so we skip boundary check below.

                // parse sign character
                int startIndex = 0;
                char firstChar = integerLiteral.charAt(0);
                boolean negative = (firstChar == '-');
                if ((firstChar == '-') || (firstChar == '+')) {
                    startIndex++;
                }
                // parse radix based on format
                int radix = 10;
                if (integerLiteral.charAt(startIndex + 1) == 'x') {
                    radix = 16;
                    startIndex += 2;
                    integerLiteral = integerLiteral.substring(startIndex);
                    if (negative) {
                        integerLiteral = '-' + integerLiteral;
                    }
                } else if (integerLiteral.charAt(startIndex) == '0') {
                    radix = 8;
                }
                // create big integer
                return new BigInteger(integerLiteral, radix);
            }
        }
    }

    @Override
    public Object visitOC_DoubleLiteral(GremlinGSParser.OC_DoubleLiteralContext ctx) {
        return Double.valueOf(ctx.getText());
    }

    @Override
    public Object visitTerminal(TerminalNode node) {
        switch (node.getSymbol().getType()) {
            case GremlinGSParser.StringLiteral:
                return StringEscapeUtils.unescapeJava(stripQuotes(node.getText()));
            case GremlinGSParser.NULL:
                return null;
            default:
                throw new UnsupportedOperationException(
                        "terminal node type " + node.getSymbol() + " is unsupported yet");
        }
    }

    @Override
    public Object visitOC_ListLiteral(GremlinGSParser.OC_ListLiteralContext ctx) {
        return ctx.oC_Expression().stream().map(this::visit).collect(Collectors.toList());
    }

    @Override
    public Object visitOC_UnaryAddOrSubtractExpression(
            GremlinGSParser.OC_UnaryAddOrSubtractExpressionContext ctx) {
        Object value = visit(ctx.oC_ListOperatorExpression());
        if (ctx.getChildCount() > 0
                && ctx.getChild(0) instanceof TerminalNode
                && ctx.getChild(0).getText().equals("-")) {
            Preconditions.checkArgument(
                    value instanceof Number, "unary minus can only be applied to number");
            if (value instanceof Integer) {
                value = Math.negateExact((Integer) value);
            } else if (value instanceof Long) {
                value = Math.negateExact((Long) value);
            } else if (value instanceof Double) {
                Double doubleVal = (Double) value;
                // Handle positive and negative zero
                if (Double.compare(0.0d, doubleVal) == 0 || Double.compare(-0.0d, doubleVal) == 0) {
                    return 0.0d;
                }
                // Handle infinity and NaN
                if (Double.isInfinite(doubleVal) || Double.isNaN(doubleVal)) {
                    throw new ArithmeticException("double overflow");
                }
                value = -doubleVal;
            } else if (value instanceof Float) {
                Float floatVal = (Float) value;
                // Handle positive and negative zero
                if (Float.compare(0.0f, floatVal) == 0 || Float.compare(-0.0f, floatVal) == 0) {
                    return 0.0f;
                }
                // Handle infinity and NaN
                if (Float.isInfinite(floatVal) || Double.isNaN(floatVal)) {
                    throw new ArithmeticException("float overflow");
                }
                value = -floatVal;
            } else if (value instanceof BigInteger) {
                value = ((BigInteger) value).negate();
            } else if (value instanceof BigDecimal) {
                value = ((BigDecimal) value).negate();
            } else {
                throw new UnsupportedOperationException(
                        "unsupported number type " + value.getClass() + " in unary minus operator");
            }
        }
        return value;
    }

    /**
     * Remove single/double quotes around String literal
     *
     * @param quotedString : quoted string
     * @return quotes stripped string
     */
    private String stripQuotes(final String quotedString) {
        return quotedString.substring(1, quotedString.length() - 1);
    }
}
