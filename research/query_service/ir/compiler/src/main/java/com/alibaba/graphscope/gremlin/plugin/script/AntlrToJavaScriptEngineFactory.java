/*
 * This file is referred and derived from project apache/tinkerpop
 *
 * https://github.com/apache/tinkerpop/blob/3.5.1/gremlin-groovy/src/main/java/org/apache/tinkerpop/gremlin/groovy/jsr223/GremlinGroovyScriptEngineFactory.java
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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinScriptEngineFactory;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;

import java.util.Collections;
import java.util.List;

public class AntlrToJavaScriptEngineFactory extends AbstractGremlinScriptEngineFactory {
    public static final String ENGINE_NAME = "antlr-to-java";
    private static final String LANGUAGE_NAME = "antlr-to-java";
    private static final String PLAIN = "plain";
    private static final List<String> EXTENSIONS = Collections.singletonList("gremlin");

    public AntlrToJavaScriptEngineFactory() {
        super(ENGINE_NAME, LANGUAGE_NAME, EXTENSIONS, Collections.singletonList(PLAIN));
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
        return new AntlrToJavaScriptEngine();
    }
}
