/*
 * This file is referred and derived from project apache/tinkerpop
 *
 * https://github.com/apache/tinkerpop/blob/3.5.1/gremlin-groovy/src/main/java/org/apache/tinkerpop/gremlin/groovy/jsr223/GremlinGroovyScriptEngine.java
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

package com.alibaba.graphscope.gremlin.plugin.script;

import com.alibaba.graphscope.common.antlr4.SyntaxErrorListener;
import com.alibaba.graphscope.gremlin.antlr4.GremlinAntlrToJava;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineFactory;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSLexer;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSParser;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;

import javax.script.*;

public class AntlrToJavaScriptEngine extends AbstractScriptEngine implements GremlinScriptEngine {
    private Logger logger = LoggerFactory.getLogger(AntlrToJavaScriptEngine.class);
    private volatile AntlrToJavaScriptEngineFactory factory;

    @Override
    public Object eval(String script, ScriptContext ctx) {
        logger.debug("antlr start to eval \"{}\"", script);
        Bindings globalBindings = ctx.getBindings(ScriptContext.ENGINE_SCOPE);
        GraphTraversalSource g = (GraphTraversalSource) globalBindings.get("g");
        GremlinAntlrToJava antlrToJava = GremlinAntlrToJava.getInstance(g);

        GremlinGSLexer lexer = new GremlinGSLexer(CharStreams.fromString(script));
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
        return antlrToJava.visit(parser.query());
    }

    @Override
    public Object eval(Reader reader, ScriptContext context) {
        throw new NotImplementedException("use eval(String, ScriptContext) instead");
    }

    @Override
    public Traversal.Admin eval(Bytecode bytecode, Bindings bindings, String traversalSource) {
        throw new NotImplementedException("use eval(String, ScriptContext) instead");
    }

    @Override
    public Bindings createBindings() {
        return new SimpleBindings();
    }

    @Override
    public GremlinScriptEngineFactory getFactory() {
        if (this.factory == null) {
            synchronized (this) {
                if (this.factory == null) {
                    this.factory = new AntlrToJavaScriptEngineFactory();
                }
            }
        }
        return this.factory;
    }
}
