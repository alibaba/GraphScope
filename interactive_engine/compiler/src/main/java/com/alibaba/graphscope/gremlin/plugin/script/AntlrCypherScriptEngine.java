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

package com.alibaba.graphscope.gremlin.plugin.script;

import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;

import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.SimpleBindings;

public class AntlrCypherScriptEngine extends AbstractScriptEngine implements GremlinScriptEngine {
    private static final Logger logger = LoggerFactory.getLogger(AntlrCypherScriptEngine.class);
    private volatile AntlrCypherScriptEngineFactory factory;
    private final CypherAntlr4Parser cypherParser;

    public AntlrCypherScriptEngine() {
        this.cypherParser = new CypherAntlr4Parser();
    }

    @Override
    public Object eval(String script, ScriptContext ctx) {
        logger.debug("antlr-cypher start to eval \"{}\"", script);
        return this.cypherParser.parse(script);
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
                    this.factory = new AntlrCypherScriptEngineFactory();
                }
            }
        }
        return this.factory;
    }
}
