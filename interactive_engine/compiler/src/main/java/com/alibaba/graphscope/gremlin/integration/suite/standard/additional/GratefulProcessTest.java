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
package com.alibaba.graphscope.gremlin.integration.suite.standard.additional;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversal;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

@RunWith(GremlinProcessRunner.class)
public abstract class GratefulProcessTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Long> get_g_V_both_both_count();

    public abstract Traversal<Vertex, Long> get_g_V_path_expand_outX_timesX3X_count();

    public abstract Traversal<Vertex, Long> get_g_V_path_expand_outX_timesX8X_count();

    public abstract Traversal<Vertex, Map<Object, Object>>
            get_g_V_hasXsong_name_OHBOYX_outXfollowedByX_outXfollowedByX_order_byXperformancesX_byXsongType_descX();

    public abstract Traversal<Vertex, String>
            get_g_V_hasLabelXsongX_order_byXperformances_descX_byXnameX_rangeX110_120X_name();

    @Test
    @LoadGraphWith(GraphData.GRATEFUL)
    public void g_V_both_both_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_both_both_count();
        printTraversalForm(traversal);
        assertEquals(new Long(1406914), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(GraphData.GRATEFUL)
    public void g_V_repeatXoutX_timesX3X_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_path_expand_outX_timesX3X_count();
        printTraversalForm(traversal);
        assertEquals(new Long(14465066L), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(GraphData.GRATEFUL)
    public void g_V_repeatXoutX_timesX8X_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_path_expand_outX_timesX8X_count();
        printTraversalForm(traversal);
        assertEquals(new Long(2505037961767380L), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(GraphData.GRATEFUL)
    public void
            g_V_hasXsong_name_OHBOYX_outXfollowedByX_outXfollowedByX_order_byXperformancesX_byXsongType_descX() {
        final Traversal<Vertex, Map<Object, Object>> traversal =
                get_g_V_hasXsong_name_OHBOYX_outXfollowedByX_outXfollowedByX_order_byXperformancesX_byXsongType_descX();
        printTraversalForm(traversal);
        int counter = 0;
        String lastSongType = "a";
        int lastPerformances = Integer.MIN_VALUE;
        while (traversal.hasNext()) {
            final Map<Object, Object> valueMap = traversal.next();
            final String currentSongType = (String) valueMap.get("songType");
            final int currentPerformances = (Integer) valueMap.get("performances");
            assertTrue(
                    currentPerformances == lastPerformances
                            || currentPerformances > lastPerformances);
            if (currentPerformances == lastPerformances)
                assertTrue(
                        currentSongType.equals(lastSongType)
                                || currentSongType.compareTo(lastSongType) < 0);
            lastSongType = currentSongType;
            lastPerformances = currentPerformances;
            counter++;
        }
        assertEquals(144, counter);
    }

    public static class Traversals extends GratefulProcessTest {
        @Override
        public Traversal<Vertex, Long> get_g_V_both_both_count() {
            return g.V().both().both().count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_path_expand_outX_timesX3X_count() {
            return ((IrCustomizedTraversal) g.V()).out("3..4").count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_path_expand_outX_timesX8X_count() {
            return ((IrCustomizedTraversal) g.V()).out("8..9").count();
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>>
                get_g_V_hasXsong_name_OHBOYX_outXfollowedByX_outXfollowedByX_order_byXperformancesX_byXsongType_descX() {
            return g.V().has("song", "name", "OH BOY")
                    .out("followedBy")
                    .out("followedBy")
                    .as("a")
                    .select("a")
                    .valueMap("performances", "songType")
                    .order()
                    .by(__.select("a").by("performances"))
                    .by(__.select("a").by("songType"), desc);
        }

        @Override
        public Traversal<Vertex, String>
                get_g_V_hasLabelXsongX_order_byXperformances_descX_byXnameX_rangeX110_120X_name() {
            return g.V().hasLabel("song")
                    .order()
                    .by("performances", desc)
                    .by("name")
                    .range(110, 120)
                    .values("name");
        }
    }
}
