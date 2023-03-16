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

package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.gremlin.plugin.script.SyntaxErrorListener;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSLexer;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSParser;
import org.junit.Test;

public class GremlinGSTest {
    @Test
    public void test() {
        CharStream streams = CharStreams.fromString("g.V(a)");
        GremlinGSLexer lexer = new GremlinGSLexer(streams);
        // reset error listeners on lexer
        lexer.removeErrorListeners();
        lexer.addErrorListener(new SyntaxErrorListener());
        final GremlinGSParser parser = new GremlinGSParser(new CommonTokenStream(lexer));
        // setup error handler on parser
        parser.setErrorHandler(new DefaultErrorStrategy());
        // reset error listeners on parser
        parser.removeErrorListeners();
        parser.addErrorListener(new SyntaxErrorListener());
        parser.getInterpreter().setPredictionMode(PredictionMode.LL);
        parser.query();
    }
}
