package com.alibaba.graphscope.gremlin.antlr4;

import com.alibaba.graphscope.gremlin.exception.InvalidGremlinScriptException;
import com.alibaba.graphscope.gremlin.plugin.script.AntlrToJavaScriptEngine;
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
        GraphTraversalSource g = graph.traversal();
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
    public void g_V_id_test() {
        try {
            scriptEngine.eval("g.V(1)", context);
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
    public void g_V_otherV_test() {
        try {
            scriptEngine.eval("g.V().otherV()", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_inV_test() {
        try {
            scriptEngine.eval("g.V().inV()", context);
        } catch (InvalidGremlinScriptException e) {
            // expected error
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_outV_test() {
        try {
            scriptEngine.eval("g.V().outV()", context);
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
}
