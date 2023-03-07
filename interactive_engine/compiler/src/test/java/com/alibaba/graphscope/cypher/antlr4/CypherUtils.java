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

package com.alibaba.graphscope.cypher.antlr4;

import com.alibaba.graphscope.common.ir.IrUtils;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.alibaba.graphscope.cypher.antlr4.visitor.CypherToAlgebraVisitor;
import com.alibaba.graphscope.grammar.CypherGSLexer;
import com.alibaba.graphscope.grammar.CypherGSParser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.calcite.rel.RelNode;

public abstract class CypherUtils {
    public static final CypherToAlgebraVisitor mockVisitor(GraphBuilder builder) {
        return new CypherToAlgebraVisitor(builder);
    }

    /**
     * prepare graph operators for test
     * @return
     */
    public static final GraphBuilder mockGraphBuilder() {
        GraphBuilder builder = IrUtils.mockGraphBuilder();
        RelNode sentence =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows"),
                                        "b"))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        new LabelConfig(false).addLabel("person"),
                                        "c"))
                        .build();
        builder.match(sentence, GraphOpt.Match.INNER);
        return builder;
    }

    public static final CypherGSParser mockParser(String query) {
        CypherGSLexer lexer = new CypherGSLexer(CharStreams.fromString(query));
        lexer.removeErrorListeners();
        lexer.addErrorListener(
                new BaseErrorListener() {
                    @Override
                    public void syntaxError(
                            final Recognizer<?, ?> recognizer,
                            final Object offendingSymbol,
                            final int line,
                            final int charPositionInLine,
                            final String msg,
                            final RecognitionException e) {
                        throw new ParseCancellationException();
                    }
                });
        CypherGSParser parser = new CypherGSParser(new CommonTokenStream(lexer));
        // setup error handler on parser
        parser.setErrorHandler(new BailErrorStrategy());
        parser.getInterpreter().setPredictionMode(PredictionMode.LL);
        return parser;
    }
}
