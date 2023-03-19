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

import org.antlr.v4.runtime.misc.ParseCancellationException;
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
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 1]; msg is: [no viable alternative at"
                            + " input 'g111']",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V111_test() {
        try {
            scriptEngine.eval("g.V111()", context);
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 3]; msg is: [no viable alternative at"
                            + " input 'g.V111']",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_outX_test() {
        try {
            scriptEngine.eval("g.V().outX()", context);
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 9]; msg is: [token recognition error"
                            + " at: 'X']",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_out_id_test() {
        try {
            scriptEngine.eval("g.V().out(1)", context);
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 10]; msg is: [no viable alternative"
                            + " at input '1']",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_out_limit_str_test() {
        try {
            scriptEngine.eval("g.V().out().limit('xxx')", context);
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 18]; msg is: [mismatched input"
                            + " ''xxx'' expecting IntegerLiteral]",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_has_key_id_test() {
        try {
            scriptEngine.eval("g.V().has(1, 1)", context);
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 10]; msg is: [no viable alternative"
                            + " at input 'has(1']",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_valueMap_int_test() {
        try {
            scriptEngine.eval("g.V().valueMap(1)", context);
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 15]; msg is: [no viable alternative"
                            + " at input '1']",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_select_none_test() {
        try {
            scriptEngine.eval("g.V().select()", context);
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 13]; msg is: [no viable alternative"
                            + " at input 'select()']",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_select_by_keys_test() {
        try {
            scriptEngine.eval("g.V().select().by('name', 'id')", context);
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 13]; msg is: [no viable alternative"
                            + " at input 'select()']",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_order_by_orders_test() {
        try {
            scriptEngine.eval("g.V().order().by(asc, desc)", context);
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 20]; msg is: [mismatched input ','"
                            + " expecting ')']",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_order_by_keys_test() {
        try {
            scriptEngine.eval("g.V().order().by('name', 'id')", context);
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 25]; msg is: [mismatched input ''id''"
                            + " expecting {'asc', 'Order.asc', 'desc', 'Order.desc', 'shuffle',"
                            + " 'Order.shuffle'}]",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_as_invalid_test() {
        try {
            scriptEngine.eval("g.V().as('a', 'b')", context);
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 12]; msg is: [mismatched input ','"
                            + " expecting ')']",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_str_id_test() {
        try {
            scriptEngine.eval("g.V(\"1\")", context);
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 4]; msg is: [no viable alternative at"
                            + " input 'g.V(\"1\"']",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_E_str_id_test() {
        try {
            scriptEngine.eval("g.E(\"1\")", context);
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 4]; msg is: [no viable alternative at"
                            + " input 'g.E(\"1\"']",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_hasId_str_id_test() {
        try {
            scriptEngine.eval("g.V().hasId(\"1\")", context);
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 12]; msg is: [mismatched input"
                            + " '\"1\"' expecting {IntegerLiteral, '['}]",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    // g.V().group().by('name', 'age') is unsupported
    @Test
    public void g_V_group_by_name_age_test() {
        try {
            scriptEngine.eval("g.V().group().by('name', 'age')", context);
        } catch (ParseCancellationException e) {
            // expected error
            Assert.assertEquals(
                    "syntax error occurs at [line: 1, column: 23]; msg is: [no viable alternative"
                            + " at input 'group().by('name',']",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    // invalid composition of operators
    @Test
    public void g_V_subgraph() {
        try {
            scriptEngine.eval("g.V().subgraph('X')", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            Assert.assertEquals(
                    "edge induced subgraph should follow an edge output operator [E, inE, outE,"
                            + " bothE]",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    // invalid composition of operators
    @Test
    public void g_V_out_endV() {
        try {
            scriptEngine.eval("g.V().out().endV()", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            Assert.assertEquals(
                    "endV should follow a path expand operator [out('$1..$2'), in('$1..$2'),"
                            + " both('$1..$2')]",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }
}
