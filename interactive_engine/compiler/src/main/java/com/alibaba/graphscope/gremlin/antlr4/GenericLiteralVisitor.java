/*
 * This file is referred and derived from project apache/tinkerpop
 *
 * https://github.com/apache/tinkerpop/blob/master/gremlin-core/src/main/java/org/apache/tinkerpop/gremlin/language/grammar/GenericLiteralVisitor.java
 *
 * which has the following license:
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.alibaba.graphscope.gremlin.antlr4;

import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Visitor class to handle generic literal. All visitor methods return type is Object. It maybe used as a singleton
 * in cases where a {@link Traversal} object is not expected, otherwise a new instance must be constructed.
 */
public class GenericLiteralVisitor extends GremlinGSBaseVisitor<Object> {

    private static GenericLiteralVisitor instance;

    private GenericLiteralVisitor() {}

    public static GenericLiteralVisitor getInstance() {
        if (instance == null) {
            instance = new GenericLiteralVisitor();
        }
        return instance;
    }

    /**
     * Parse a string literal context and return the string literal
     */
    public static String getStringLiteral(
            final GremlinGSParser.StringLiteralContext stringLiteral) {
        return (String) (getInstance().visitStringLiteral(stringLiteral));
    }

    /**
     * Parse a boolean literal context and return the boolean literal
     */
    public static boolean getBooleanLiteral(
            final GremlinGSParser.BooleanLiteralContext booleanLiteral) {
        return (boolean) (getInstance().visitBooleanLiteral(booleanLiteral));
    }

    /**
     * Parse a String literal list context and return a string array
     */
    public static String[] getStringLiteralList(
            final GremlinGSParser.StringLiteralListContext stringLiteralList) {
        if (stringLiteralList == null || stringLiteralList.stringLiteralExpr() == null) {
            return new String[0];
        }
        return stringLiteralList.stringLiteralExpr().stringLiteral().stream()
                .filter(Objects::nonNull)
                .map(stringLiteral -> getInstance().visitStringLiteral(stringLiteral))
                .toArray(String[]::new);
    }

    /**
     * Parse a String literal expr context and return a string array
     */
    public static String[] getStringLiteralExpr(
            final GremlinGSParser.StringLiteralExprContext stringLiteralExpr) {
        return stringLiteralExpr.stringLiteral().stream()
                .filter(Objects::nonNull)
                .map(stringLiteral -> getInstance().visitStringLiteral(stringLiteral))
                .toArray(String[]::new);
    }

    /**
     * Parse a generic literal list, and return an object array
     */
    public static Object[] getGenericLiteralList(
            final GremlinGSParser.GenericLiteralListContext objectLiteralList) {
        if (objectLiteralList == null || objectLiteralList.genericLiteralExpr() == null) {
            return new Object[0];
        }
        return objectLiteralList.genericLiteralExpr().genericLiteral().stream()
                .filter(Objects::nonNull)
                .map(genericLiteral -> getInstance().visitGenericLiteral(genericLiteral))
                .toArray(Object[]::new);
    }

    /**
     * Parse a Integer literal list context and return a Integer array
     */
    public static Object[] getIntegerLiteralList(
            final GremlinGSParser.IntegerLiteralListContext integerLiteralList) {
        if (integerLiteralList == null || integerLiteralList.integerLiteralExpr() == null) {
            return new Object[0];
        }
        return getIntegerLiteralExpr(integerLiteralList.integerLiteralExpr());
    }

    public static Object[] getIntegerLiteralExpr(
            GremlinGSParser.IntegerLiteralExprContext integerLiteralExpr) {
        return integerLiteralExpr.integerLiteral().stream()
                .filter(Objects::nonNull)
                .map(integerLiteral -> getInstance().visitIntegerLiteral(integerLiteral))
                .toArray(Object[]::new);
    }

    /**
     * Remove single/double quotes around String literal
     *
     * @param quotedString : quoted string
     * @return quotes stripped string
     */
    private static String stripQuotes(final String quotedString) {
        return quotedString.substring(1, quotedString.length() - 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitGenericLiteralList(final GremlinGSParser.GenericLiteralListContext ctx) {
        return visitChildren(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitGenericLiteralExpr(final GremlinGSParser.GenericLiteralExprContext ctx) {
        final int childCount = ctx.getChildCount();
        switch (childCount) {
            case 0:
                // handle empty expression
                return new Object[0];
            case 1:
                // handle single generic literal
                return visitGenericLiteral((GremlinGSParser.GenericLiteralContext) ctx.getChild(0));
            default:
                // handle multiple generic literal separated by comma
                final List<Object> genericLiterals = new ArrayList<>();
                int childIndex = 0;
                while (childIndex < ctx.getChildCount()) {
                    genericLiterals.add(
                            visitGenericLiteral(
                                    (GremlinGSParser.GenericLiteralContext)
                                            ctx.getChild(childIndex)));
                    // skip comma
                    childIndex += 2;
                }
                return genericLiterals.toArray();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitGenericLiteral(final GremlinGSParser.GenericLiteralContext ctx) {
        return visitChildren(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitIntegerLiteral(final GremlinGSParser.IntegerLiteralContext ctx) {
        String integerLiteral = ctx.getText().toLowerCase().replace("_", "");
        // handle suffix: L/l
        final int lastCharIndex = integerLiteral.length() - 1;
        if (integerLiteral.charAt(lastCharIndex) == 'l') {
            integerLiteral = integerLiteral.substring(0, lastCharIndex);

            return Long.decode(integerLiteral);
        }

        try {
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
                final char firstChar = integerLiteral.charAt(0);
                final boolean negative = (firstChar == '-');
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

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitFloatLiteral(final GremlinGSParser.FloatLiteralContext ctx) {
        final String floatLiteral = ctx.getText().toLowerCase();

        // check suffix
        final char lastCharacter = floatLiteral.charAt(floatLiteral.length() - 1);
        if (Character.isDigit(lastCharacter)) {
            // if there is no suffix, parse it as BigDecimal
            return new BigDecimal(floatLiteral);
        }

        if (lastCharacter == 'f') {
            // parse F/f suffix as Float
            return new Float(ctx.getText());
        } else {
            // parse D/d suffix as Double
            return new Double(floatLiteral);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitBooleanLiteral(final GremlinGSParser.BooleanLiteralContext ctx) {
        return Boolean.valueOf(ctx.getText());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitStringLiteral(final GremlinGSParser.StringLiteralContext ctx) {
        // Using Java string unescaping because it coincides with the Groovy rules:
        // https://docs.oracle.com/javase/tutorial/java/data/characters.html
        // http://groovy-lang.org/syntax.html#_escaping_special_characters

        return StringEscapeUtils.unescapeJava(stripQuotes(ctx.getText()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitNullLiteral(final GremlinGSParser.NullLiteralContext ctx) {
        return null;
    }
}
