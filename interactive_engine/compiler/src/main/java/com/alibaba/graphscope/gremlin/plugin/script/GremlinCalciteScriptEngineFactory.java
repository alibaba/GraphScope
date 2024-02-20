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

import com.alibaba.graphscope.common.ir.tools.QueryCache;
import com.alibaba.graphscope.common.store.IrMeta;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinScriptEngineFactory;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.io.Reader;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;

public class GremlinCalciteScriptEngineFactory extends AbstractGremlinScriptEngineFactory {
    private static final String PLAIN = "plain";
    private static final List<String> EXTENSIONS = Collections.singletonList("gremlin");

    public static final String LANGUAGE_NAME = "antlr_gremlin_calcite";

    public GremlinCalciteScriptEngineFactory() {
        super(LANGUAGE_NAME, LANGUAGE_NAME, EXTENSIONS, Collections.singletonList(PLAIN));
    }

    @Override
    public String getMethodCallSyntax(String obj, String m, String... args) {
        return null;
    }

    @Override
    public String getOutputStatement(String toDisplay) {
        return null;
    }

    @Override
    public GremlinScriptEngine getScriptEngine() {
        return new ScriptEngine();
    }

    public static class ScriptEngine extends AbstractScriptEngine implements GremlinScriptEngine {
        private volatile GremlinScriptEngineFactory factory;

        @Override
        public Object eval(String script, ScriptContext ctx) throws ScriptException {
            try {
                Bindings globalBindings = ctx.getBindings(ScriptContext.ENGINE_SCOPE);
                QueryCache queryCache = (QueryCache) globalBindings.get("graph.query.cache");
                IrMeta irMeta = (IrMeta) globalBindings.get("graph.meta");
                QueryCache.Key cacheKey = queryCache.createKey(script, irMeta);
                return queryCache.get(cacheKey);
            } catch (ExecutionException e) {
                return new RuntimeException(e);
            }
        }

        @Override
        public Object eval(Reader reader, ScriptContext context) throws ScriptException {
            return null;
        }

        @Override
        public Bindings createBindings() {
            return null;
        }

        @Override
        public GremlinScriptEngineFactory getFactory() {
            if (this.factory == null) {
                synchronized (this) {
                    if (this.factory == null) {
                        this.factory = new GremlinCalciteScriptEngineFactory();
                    }
                }
            }
            return this.factory;
        }

        @Override
        public Traversal.Admin eval(Bytecode bytecode, Bindings bindings, String traversalSource)
                throws ScriptException {
            return null;
        }
    }
}
