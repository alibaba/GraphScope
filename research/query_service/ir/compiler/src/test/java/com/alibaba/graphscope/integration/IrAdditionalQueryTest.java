package com.alibaba.graphscope.integration;

import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

public abstract class IrAdditionalQueryTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_hasXlangX_group_byXlangX_byXcountX();

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_group_byXageX();

    @Test
    public void g_V_hasXlangX_group_byXlangX_byXcountX() {
        Traversal<Vertex, Map<Object, Object>> traversal = this.get_g_V_hasXlangX_group_byXlangX_byXcountX();
        this.printTraversalForm(traversal);
        Map<String, Long> map = (Map) traversal.next();
        Assert.assertEquals(1L, (long) map.size());
        Assert.assertTrue(map.containsKey("java"));
        Assert.assertEquals(Long.valueOf(2L), map.get("java"));
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_V_group_byXageX() {
        Traversal<Vertex, Map<Object, Object>> traversal = this.get_g_V_group_byXageX();
        this.printTraversalForm(traversal);
        Map<Integer, Collection<Vertex>> map = (Map) traversal.next();
        Assert.assertEquals(5L, (long) map.size());
        map.forEach((key, values) -> {
            if (null == key) {
                Assert.assertEquals(2L, (long) values.size());
            } else {
                Assert.assertEquals(1L, (long) values.size());
            }

        });
        Assert.assertFalse(traversal.hasNext());
    }

    public static class Traversals extends IrAdditionalQueryTest {
        public Traversals() {
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_hasXlangX_group_byXlangX_byXcountX() {
            return this.g.V(new Object[0]).has("lang").group().by("lang").by(__.count());
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_group_byXageX() {
            return this.g.V(new Object[0]).group().by("age");
        }
    }
}
