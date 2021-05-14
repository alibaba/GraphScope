package com.alibaba.graphscope.gaia;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GremlinProcessRunner.class)
public abstract class SmallCountTest extends AbstractGremlinProcessTest {
    private static final Logger logger = LoggerFactory.getLogger(SmallCountTest.class);

    public abstract Traversal<Vertex, Long> g_V_both_both_count();

    public abstract Traversal<Vertex, Path> get_g_V_emit_repeatXoutX_timesX2X_path();

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void test1() {
        Traversal<Vertex, Long> traversal = g_V_both_both_count();
        Assert.assertEquals(new Long(1406914L), traversal.next());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void test2() {
        Traversal<Vertex, Path> traversal = get_g_V_emit_repeatXoutX_timesX2X_path();
        // Assert.assertEquals(new Long(1406914L), traversal.next());
        while (traversal.hasNext()) {
            Path path = traversal.next();
            logger.info("path is {}", path);
        }
    }

    public static class Traversals extends SmallCountTest {

        @Override
        public Traversal<Vertex, Long> g_V_both_both_count() {
            Traversal<Vertex, Long> result = this.g.V(new Object[0]).out().count();
            return result;
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_emit_repeatXoutX_timesX2X_path() {
            return this.g.V(new Object[0]).emit().repeat(__.out(new String[0])).times(2).path();
        }
    }
}
