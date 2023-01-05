/**
 * This file is referred and derived from project apache/tinkerpop
 *
 *   https://github.com/apache/tinkerpop/blob/master/gremlin-test/src/main/java/org/apache/tinkerpop/gremlin/process/traversal/step/map/CountTest.java
 *
 * which has the following license:
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.graphscope.function.test.gremlin;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

@RunWith(GremlinProcessRunner.class)
public abstract class GratefulProcessTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Long> get_g_V_both_both_count();

    public abstract Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX3X_count();

    public abstract Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX8X_count();

    public abstract Traversal<Vertex, Vertex>
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
        final Traversal<Vertex, Long> traversal = get_g_V_repeatXoutX_timesX3X_count();
        printTraversalForm(traversal);
        assertEquals(new Long(14465066L), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(GraphData.GRATEFUL)
    public void g_V_repeatXoutX_timesX8X_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_repeatXoutX_timesX8X_count();
        printTraversalForm(traversal);
        assertEquals(new Long(2505037961767380L), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(GraphData.GRATEFUL)
    public void
            g_V_hasXsong_name_OHBOYX_outXfollowedByX_outXfollowedByX_order_byXperformancesX_byXsongType_descX() {
        final Traversal<Vertex, Vertex> traversal =
                get_g_V_hasXsong_name_OHBOYX_outXfollowedByX_outXfollowedByX_order_byXperformancesX_byXsongType_descX();
        printTraversalForm(traversal);
        int counter = 0;
        String lastSongType = "a";
        int lastPerformances = Integer.MIN_VALUE;
        while (traversal.hasNext()) {
            final Vertex vertex = traversal.next();
            final String currentSongType = vertex.value("songType");
            final int currentPerformances = vertex.value("performances");
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

    @Test
    @LoadGraphWith(GraphData.GRATEFUL)
    public void g_V_hasLabelXsongX_order_byXperformances_descX_byXnameX_rangeX110_120X_name() {
        final Traversal<Vertex, String> traversal =
                get_g_V_hasLabelXsongX_order_byXperformances_descX_byXnameX_rangeX110_120X_name();
        printTraversalForm(traversal);
        checkOrderedResults(
                Arrays.asList(
                        "WANG DANG DOODLE",
                        "THE ELEVEN",
                        "WAY TO GO HOME",
                        "FOOLISH HEART",
                        "GIMME SOME LOVING",
                        "DUPREES DIAMOND BLUES",
                        "CORRINA",
                        "PICASSO MOON",
                        "KNOCKING ON HEAVENS DOOR",
                        "MEMPHIS BLUES"),
                traversal);
    }

    public static class Traversals extends GratefulProcessTest {
        @Override
        public Traversal<Vertex, Long> get_g_V_both_both_count() {
            return g.V().both().both().count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX3X_count() {
            return g.V().repeat(out()).times(3).count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX8X_count() {
            return g.V().repeat(out()).times(8).count();
        }

        @Override
        public Traversal<Vertex, Vertex>
                get_g_V_hasXsong_name_OHBOYX_outXfollowedByX_outXfollowedByX_order_byXperformancesX_byXsongType_descX() {
            return g.V().has("song", "name", "OH BOY")
                    .out("followedBy")
                    .out("followedBy")
                    .order()
                    .by("performances")
                    .by("songType", desc);
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
