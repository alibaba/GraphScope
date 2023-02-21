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

package com.alibaba.graphscope.calcite.antlr4.visitor;

import com.alibaba.graphscope.grammar.CypherGSBaseVisitor;
import com.alibaba.graphscope.grammar.CypherGSParser;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.text.StringEscapeUtils;

import java.math.BigInteger;

public class LiteralVisitor extends CypherGSBaseVisitor<Object> {
    public static final LiteralVisitor INSTANCE = new LiteralVisitor();

    private LiteralVisitor() {}

    @Override
    public Object visitOC_BooleanLiteral(CypherGSParser.OC_BooleanLiteralContext ctx) {
        return Boolean.valueOf(ctx.getText());
    }

    @Override
    public Object visitOC_IntegerLiteral(CypherGSParser.OC_IntegerLiteralContext ctx) {
        String integerLiteral = ctx.getText().toLowerCase();
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
    public Object visitOC_DoubleLiteral(CypherGSParser.OC_DoubleLiteralContext ctx) {
        String floatLiteral = ctx.getText().toLowerCase();
        return Double.valueOf(floatLiteral);
    }

    @Override
    public Object visitTerminal(TerminalNode node) {
        switch (node.getSymbol().getType()) {
            case CypherGSParser.StringLiteral:
                return StringEscapeUtils.unescapeJava(stripQuotes(node.getText()));
            case CypherGSParser.NULL:
                return null;
            default:
                throw new UnsupportedOperationException(
                        "terminal node type " + node.getSymbol().getType() + " is unsupported yet");
        }
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
