package com.alibaba.graphscope.gaia;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

@RunWith(GremlinProcessRunner.class)
public abstract class SmallTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Vertex> g_VXv1X_hasXage_gt_30X();

    public abstract Traversal<Vertex, Edge> g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectXhereX();

    public abstract Traversal<Vertex, Path> g_VX1X_name_path();

    public abstract GraphTraversal<Vertex, Map<Object, Object>> g_V_hasXlangX_group_byXlangX_byXcountX();

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void test1() {
        Traversal<Vertex, Map<Object, Object>> result = g_V_hasXlangX_group_byXlangX_byXcountX();
        result.hasNext();
    }

    public static class Traversals extends SmallTest {
        @Override
        public Traversal<Vertex, Vertex> g_VXv1X_hasXage_gt_30X() {
            Vertex v1Id = (Vertex) this.graphProvider.traversal(graph).V(new Object[0]).has("name", "marko").toList().get(0);
            return this.g.V(new Object[]{v1Id}).has("age", P.gt(30));
        }

        @Override
        public Traversal<Vertex, Edge> g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectXhereX() {
            Vertex v1Id = (Vertex) this.graphProvider.traversal(graph).V(new Object[0]).has("name", "marko").toList().get(0);
            return g.V(v1Id).outE("knows").as("here").has("weight", 1.0d).as("fake").inV().has("name", "josh").<Edge>select("here");
        }

        @Override
        public Traversal<Vertex, Path> g_VX1X_name_path() {
            Vertex v1Id = (Vertex) this.graphProvider.traversal(graph).V(new Object[0]).has("name", "marko").toList().get(0);
            return this.g.V(new Object[]{v1Id}).values(new String[]{"name"}).path();
        }

        @Override
        public GraphTraversal<Vertex, Map<Object, Object>> g_V_hasXlangX_group_byXlangX_byXcountX() {
            return this.g.V(new Object[0]).has("lang").group().by("lang").by(__.count());
        }
    }
}
