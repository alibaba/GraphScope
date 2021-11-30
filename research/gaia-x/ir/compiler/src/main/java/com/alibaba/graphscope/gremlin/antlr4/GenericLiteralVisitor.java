package com.alibaba.graphscope.gremlin.antlr4;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2BaseVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2Parser;
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
public class GenericLiteralVisitor extends GremlinGS_0_2BaseVisitor<Object> {

    private static GenericLiteralVisitor instance;

    private GenericLiteralVisitor() {
    }

    public static GenericLiteralVisitor getInstance() {
        if (instance == null) {
            instance = new GenericLiteralVisitor();
        }
        return instance;
    }

    /**
     * Parse a string literal context and return the string literal
     */
    public static String getStringLiteral(final GremlinGS_0_2Parser.StringLiteralContext stringLiteral) {
        return (String) (getInstance().visitStringLiteral(stringLiteral));
    }

    /**
     * Parse a boolean literal context and return the boolean literal
     */
    public static boolean getBooleanLiteral(final GremlinGS_0_2Parser.BooleanLiteralContext booleanLiteral) {
        return (boolean) (getInstance().visitBooleanLiteral(booleanLiteral));
    }

    /**
     * Parse a String literal list context and return a string array
     */
    public static String[] getStringLiteralList(final GremlinGS_0_2Parser.StringLiteralListContext stringLiteralList) {
        if (stringLiteralList == null || stringLiteralList.stringLiteralExpr() == null) {
            return new String[0];
        }
        return stringLiteralList.stringLiteralExpr().stringLiteral()
                .stream()
                .filter(Objects::nonNull)
                .map(stringLiteral -> getInstance().visitStringLiteral(stringLiteral))
                .toArray(String[]::new);
    }

    /**
     * Parse a generic literal list, and return an object array
     */
    public static Object[] getGenericLiteralList(final GremlinGS_0_2Parser.GenericLiteralListContext objectLiteralList) {
        if (objectLiteralList == null || objectLiteralList.genericLiteralExpr() == null) {
            return new Object[0];
        }
        return objectLiteralList.genericLiteralExpr().genericLiteral()
                .stream()
                .filter(Objects::nonNull)
                .map(genericLiteral -> getInstance().visitGenericLiteral(genericLiteral))
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
    public Object visitGenericLiteralList(final GremlinGS_0_2Parser.GenericLiteralListContext ctx) {
        return visitChildren(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitGenericLiteralExpr(final GremlinGS_0_2Parser.GenericLiteralExprContext ctx) {
        final int childCount = ctx.getChildCount();
        switch (childCount) {
            case 0:
                // handle empty expression
                return new Object[0];
            case 1:
                // handle single generic literal
                return visitGenericLiteral((GremlinGS_0_2Parser.GenericLiteralContext) ctx.getChild(0));
            default:
                // handle multiple generic literal separated by comma
                final List<Object> genericLiterals = new ArrayList<>();
                int childIndex = 0;
                while (childIndex < ctx.getChildCount()) {
                    genericLiterals.add(visitGenericLiteral(
                            (GremlinGS_0_2Parser.GenericLiteralContext) ctx.getChild(childIndex)));
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
    public Object visitGenericLiteral(final GremlinGS_0_2Parser.GenericLiteralContext ctx) {
        return visitChildren(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitIntegerLiteral(final GremlinGS_0_2Parser.IntegerLiteralContext ctx) {
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
                // as the literal range is longer than long, the number of character should be much more than 3,
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
    public Object visitFloatLiteral(final GremlinGS_0_2Parser.FloatLiteralContext ctx) {
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
    public Object visitBooleanLiteral(final GremlinGS_0_2Parser.BooleanLiteralContext ctx) {
        return Boolean.valueOf(ctx.getText());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitStringLiteral(final GremlinGS_0_2Parser.StringLiteralContext ctx) {
        // Using Java string unescaping because it coincides with the Groovy rules:
        // https://docs.oracle.com/javase/tutorial/java/data/characters.html
        // http://groovy-lang.org/syntax.html#_escaping_special_characters
        if (ctx.NullLiteral() != null) {
            return null;
        }
        return StringEscapeUtils.unescapeJava(stripQuotes(ctx.getText()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitNullLiteral(final GremlinGS_0_2Parser.NullLiteralContext ctx) {
        return null;
    }
}
