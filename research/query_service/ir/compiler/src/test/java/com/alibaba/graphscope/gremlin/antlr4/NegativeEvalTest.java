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

package com.alibaba.graphscope.gremlin.antlr4;

import com.alibaba.graphscope.gremlin.exception.InvalidGremlinScriptException;
import com.alibaba.graphscope.gremlin.plugin.script.AntlrToJavaScriptEngine;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.SimpleBindings;
import javax.script.SimpleScriptContext;

public class NegativeEvalTest {
    private AntlrToJavaScriptEngine scriptEngine;
    private ScriptContext context;

    @Before
    public void before() {
        Graph graph = TinkerFactory.createModern();
        GraphTraversalSource g = graph.traversal(IrCustomizedTraversalSource.class);
        Bindings globalBindings = new SimpleBindings();
        globalBindings.put("g", g);
        context = new SimpleScriptContext();
        context.setBindings(globalBindings, ScriptContext.ENGINE_SCOPE);
        scriptEngine = new AntlrToJavaScriptEngine();
    }

    @Test
    public void g111_test() {
        try {
            scriptEngine.eval("g111()", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V111_test() {
        try {
            scriptEngine.eval("g.V111()", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_outX_test() {
        try {
            scriptEngine.eval("g.V().outX()", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_out_id_test() {
        try {
            scriptEngine.eval("g.V().out(1)", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_out_limit_str_test() {
        try {
            scriptEngine.eval("g.V().out().limit('xxx')", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_has_key_id_test() {
        try {
            scriptEngine.eval("g.V().has(1, 1)", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_bothV_test() {
        try {
            scriptEngine.eval("g.V().bothV()", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_valueMap_int_test() {
        try {
            scriptEngine.eval("g.V().valueMap(1)", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_select_none_test() {
        try {
            scriptEngine.eval("g.V().select()", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_select_by_keys_test() {
        try {
            scriptEngine.eval("g.V().select().by('name', 'id')", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_order_by_orders_test() {
        try {
            scriptEngine.eval("g.V().order().by(asc, desc)", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_order_by_keys_test() {
        try {
            scriptEngine.eval("g.V().order().by('name', 'id')", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_valueMap_none_test() {
        try {
            scriptEngine.eval("g.V().valueMap()", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_as_invalid_test() {
        try {
            scriptEngine.eval("g.V().as('a', 'b')", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }
}
