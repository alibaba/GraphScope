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

package com.alibaba.graphscope.cypher.antlr4.parser;

import com.alibaba.graphscope.common.antlr4.Antlr4Parser;
import com.alibaba.graphscope.common.antlr4.SyntaxErrorListener;
import com.alibaba.graphscope.grammar.CypherGSLexer;
import com.alibaba.graphscope.grammar.CypherGSParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ParseTree;

public class CypherAntlr4Parser implements Antlr4Parser {
    @Override
    public ParseTree parse(String statement) {
        CypherGSLexer lexer = new CypherGSLexer(CharStreams.fromString(statement));
        // reset error listeners on lexer
        lexer.removeErrorListeners();
        lexer.addErrorListener(new SyntaxErrorListener());
        final CypherGSParser parser = new CypherGSParser(new CommonTokenStream(lexer));
        // setup error handler on parser
        parser.setErrorHandler(new DefaultErrorStrategy());
        // reset error listeners on parser
        parser.removeErrorListeners();
        parser.addErrorListener(new SyntaxErrorListener());
        parser.getInterpreter().setPredictionMode(PredictionMode.LL);
        return parser.oC_Cypher();
    }
}
