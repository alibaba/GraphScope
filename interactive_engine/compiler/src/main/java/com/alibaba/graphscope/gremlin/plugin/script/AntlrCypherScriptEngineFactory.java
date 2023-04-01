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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinScriptEngineFactory;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;

import java.util.Collections;
import java.util.List;

public class AntlrCypherScriptEngineFactory extends AbstractGremlinScriptEngineFactory {
    private static final String PLAIN = "plain";
    private static final List<String> EXTENSIONS = Collections.singletonList("cypher");

    public static final String LANGUAGE_NAME = "antlr_cypher";

    public AntlrCypherScriptEngineFactory() {
        super(LANGUAGE_NAME, LANGUAGE_NAME, EXTENSIONS, Collections.singletonList(PLAIN));
    }

    @Override
    public String getMethodCallSyntax(String obj, String m, String... args) {
        throw new NotImplementedException();
    }

    @Override
    public String getOutputStatement(String toDisplay) {
        throw new NotImplementedException();
    }

    @Override
    public GremlinScriptEngine getScriptEngine() {
        return new AntlrCypherScriptEngine();
    }
}
