package com.alibaba.graphscope.gaia;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(GremlinProcessRunner.class)
public abstract class SmallCountTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Long> g_V_both_both_count();

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void test1() {
        Traversal<Vertex, Long> traversal = g_V_both_both_count();
        Assert.assertEquals(new Long(1406914L), traversal.next());
    }

    public static class Traversals extends SmallCountTest {

        @Override
        public Traversal<Vertex, Long> g_V_both_both_count() {
            Traversal<Vertex, Long> result = this.g.V(new Object[0]).has("name", "marko").count();
            return result;
        }
    }
}
