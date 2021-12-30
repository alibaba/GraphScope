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
package com.alibaba.maxgraph.function.test.gremlin;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public abstract class CrewProcessTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Map<String, Object>>
            get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_ascX_asXaX_outV_asXbX_selectXa_bX_byXskillX_byXnameX();

    @Test
    @LoadGraphWith(CREW)
    public void
            g_V_hasXname_gremlinX_inEXusesX_order_byXskill_ascX_asXaX_outV_asXbX_selectXa_bX_byXskillX_byXnameX() {
        final Traversal<Vertex, Map<String, Object>> traversal =
                get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_ascX_asXaX_outV_asXbX_selectXa_bX_byXskillX_byXnameX();
        printTraversalForm(traversal);
        final List<Map<String, Object>> expected =
                makeMapList(
                        2,
                        "a",
                        3,
                        "b",
                        "matthias",
                        "a",
                        4,
                        "b",
                        "marko",
                        "a",
                        5,
                        "b",
                        "stephen",
                        "a",
                        5,
                        "b",
                        "daniel");
        checkResults(expected, traversal);
    }

    public static class Traversals extends CrewProcessTest {
        @Override
        public Traversal<Vertex, Map<String, Object>>
                get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_ascX_asXaX_outV_asXbX_selectXa_bX_byXskillX_byXnameX() {
            return g.V().has("name", "gremlin")
                    .inE("uses")
                    .order()
                    .by("skill", Order.asc)
                    .as("a")
                    .outV()
                    .as("b")
                    .select("a", "b")
                    .by("skill")
                    .by("name");
        }
    }
}
