/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.function.test.gremlin;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.SINK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.List;

public abstract class SinkProcessTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Path>
            get_g_V_hasXloop_name_loopX_repeatXinX_timesX5X_path_by_name();

    public abstract Traversal<Vertex, Edge> get_g_V_hasLabelXloopsX_bothEXselfX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasLabelXloopsX_bothXselfX();

    @Test
    @LoadGraphWith(SINK)
    public void g_V_hasXloop_name_loopX_repeatXinX_timesX5X_path_by_name() {
        final Traversal<Vertex, Path> traversal =
                get_g_V_hasXloop_name_loopX_repeatXinX_timesX5X_path_by_name();
        printTraversalForm(traversal);
        final Path path = traversal.next();
        assertThat(path, contains("loop", "loop", "loop", "loop", "loop", "loop"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(SINK)
    public void g_V_hasLabelXloopsX_bothEXselfX() {
        final Traversal<Vertex, Edge> traversal = get_g_V_hasLabelXloopsX_bothEXselfX();
        printTraversalForm(traversal);

        List<Edge> edges = traversal.toList();
        assertEquals(2, edges.size());
        assertEquals(edges.get(0), edges.get(1));
    }

    @Test
    @LoadGraphWith(SINK)
    public void g_V_hasLabelXloopsX_bothXselfX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasLabelXloopsX_bothXselfX();
        printTraversalForm(traversal);

        List<Vertex> vertices = traversal.toList();
        assertEquals(2, vertices.size());
        assertEquals(vertices.get(0), vertices.get(1));
    }

    public static class Traversals extends SinkProcessTest {
        @Override
        public Traversal<Vertex, Edge> get_g_V_hasLabelXloopsX_bothEXselfX() {
            return g.V().hasLabel("loops").bothE("self");
        }

        @Override
        public Traversal<Vertex, Path>
                get_g_V_hasXloop_name_loopX_repeatXinX_timesX5X_path_by_name() {
            return g.V().has("loops", "name", "loop").repeat(__.in()).times(5).path().by("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasLabelXloopsX_bothXselfX() {
            return g.V().hasLabel("loops").both("self");
        }
    }
}
