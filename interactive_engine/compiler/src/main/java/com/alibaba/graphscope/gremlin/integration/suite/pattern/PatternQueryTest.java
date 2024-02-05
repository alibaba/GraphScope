/*
 * Copyright 2022 Alibaba Group Holding Limited.
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

package com.alibaba.graphscope.gremlin.integration.suite.pattern;

import com.alibaba.graphscope.gremlin.integration.suite.utils.__;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversal;

import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public abstract class PatternQueryTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Long> get_pattern_1_test();

    public abstract Traversal<Vertex, Long> get_pattern_2_test();

    public abstract Traversal<Vertex, Long> get_pattern_3_test();

    public abstract Traversal<Vertex, Long> get_pattern_4_test();

    public abstract Traversal<Vertex, Long> get_pattern_5_test();

    public abstract Traversal<Vertex, Long> get_pattern_6_test();

    public abstract Traversal<Vertex, Long> get_pattern_7_test();

    public abstract Traversal<Vertex, Long> get_pattern_8_test();

    public abstract Traversal<Vertex, Long> get_pattern_9_test();

    public abstract Traversal<Vertex, Long> get_pattern_10_test();

    public abstract Traversal<Vertex, Long> get_pattern_11_test();

    public abstract Traversal<Vertex, Long> get_pattern_12_test();

    public abstract Traversal<Vertex, Long> get_pattern_13_test();

    public abstract Traversal<Vertex, Long> get_pattern_14_test();

    public abstract Traversal<Vertex, Long> get_pattern_15_test();

    public abstract Traversal<Vertex, Long> get_pattern_16_test();

    public abstract Traversal<Vertex, Long> get_pattern_17_test();

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_limit_100_group_test();

    @Test
    public void run_pattern_1_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_1_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(240390L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_2_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_2_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(118209L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_3_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_3_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(1247146L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_4_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_4_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(23286L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_5_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_5_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(5596L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_6_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_6_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(43169L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_7_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_7_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(20858L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_8_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_8_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(1247146L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_9_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_9_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(23286L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_10_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_10_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(11547L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_11_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_11_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(506513L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_12_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_12_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(232854L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_13_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_13_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(1602774L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_14_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_14_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(118209L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_15_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_15_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(23286L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_16_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_16_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(23286L, traversal.next().longValue());
    }

    @Test
    public void run_pattern_17_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_17_test();
        this.printTraversalForm(traversal);
        Assert.assertEquals(17367L, traversal.next().longValue());
    }

    @Test
    public void run_g_V_limit_100_group_test() {
        Traversal<Vertex, Map<Object, Object>> traversal = this.get_g_V_limit_100_group_test();
        this.printTraversalForm(traversal);
        Map<Object, Object> map = traversal.next();
        Assert.assertEquals(100, map.size());
        Assert.assertFalse(traversal.hasNext());
    }

    public static class Traversals extends PatternQueryTest {

        // PM1
        @Override
        public Traversal<Vertex, Long> get_pattern_1_test() {
            return g.V().match(
                            __.as("a").out("KNOWS").as("b"), __.as("b").outE("KNOWS").inV().as("c"))
                    .count();
        }

        // PM2
        @Override
        public Traversal<Vertex, Long> get_pattern_2_test() {
            return g.V().match(
                            __.as("a").out("KNOWS").as("b"),
                            __.as("b")
                                    .hasLabel("PERSON")
                                    .has("gender", "male")
                                    .out("KNOWS")
                                    .as("c"))
                    .count();
        }

        // PM4
        @Override
        public Traversal<Vertex, Long> get_pattern_3_test() {
            return g.V().match(
                            __.as("a").out("KNOWS").as("b"),
                            __.as("b").out("KNOWS").as("c"),
                            __.as("c")
                                    .hasLabel("PERSON")
                                    .has("gender", "male")
                                    .outE("KNOWS")
                                    .inV()
                                    .as("d"))
                    .count();
        }

        // PM5
        @Override
        public Traversal<Vertex, Long> get_pattern_4_test() {
            return g.V().match(
                            __.as("a").out("KNOWS").as("b"),
                            __.as("b").out("KNOWS").as("c"),
                            __.as("a").out("KNOWS").as("c"))
                    .count();
        }

        // PM7
        @Override
        public Traversal<Vertex, Long> get_pattern_5_test() {
            return g.V().match(
                            __.as("a").has("gender", "male").out("KNOWS").as("b"),
                            __.as("b").has("gender", "female").out("KNOWS").as("c"),
                            __.as("a").out("KNOWS").as("c"))
                    .count();
        }

        // PM12
        @Override
        public Traversal<Vertex, Long> get_pattern_6_test() {
            return g.V().match(
                            __.as("a").out("KNOWS").as("b"),
                            __.as("b").out("KNOWS").as("c"),
                            __.as("a").out("KNOWS").as("c"),
                            __.as("c").out("KNOWS").as("d"),
                            __.as("b").out("KNOWS").as("d"))
                    .count();
        }

        // PM13
        @Override
        public Traversal<Vertex, Long> get_pattern_7_test() {
            return g.V().match(
                            __.as("a").out("KNOWS").as("b"),
                            __.as("b").has("gender", "male").outE("KNOWS").inV().as("c"),
                            __.as("a").out("KNOWS").as("c"),
                            __.as("c").out("KNOWS").as("d"),
                            __.as("b").outE("KNOWS").inV().as("d"))
                    .count();
        }

        // PM3-path
        @Override
        public Traversal<Vertex, Long> get_pattern_8_test() {
            return g.V().match(
                            ((IrCustomizedTraversal) __.as("a").out("2..3", "KNOWS"))
                                    .endV()
                                    .as("c"),
                            ((IrCustomizedTraversal)
                                            __.as("c")
                                                    .hasLabel("PERSON")
                                                    .has("gender", "male")
                                                    .out("1..2", "KNOWS"))
                                    .endV()
                                    .as("d"))
                    .count();
        }

        // PM5-path
        @Override
        public Traversal<Vertex, Long> get_pattern_9_test() {
            return g.V().match(
                            ((IrCustomizedTraversal) __.as("a").out("2..3", "KNOWS"))
                                    .endV()
                                    .as("c"),
                            __.as("a").out("KNOWS").as("c"))
                    .count();
        }

        // PM11-path
        @Override
        public Traversal<Vertex, Long> get_pattern_10_test() {
            return g.V().match(
                            ((IrCustomizedTraversal) __.as("a").out("1..2", "KNOWS"))
                                    .endV()
                                    .as("b"),
                            ((IrCustomizedTraversal)
                                            __.as("b").has("gender", "female").outE("KNOWS"))
                                    .inV()
                                    .as("c"),
                            ((IrCustomizedTraversal) __.as("a").out("1..2", "KNOWS"))
                                    .endV()
                                    .as("c"))
                    .count();
        }

        // fuzzy pattern
        @Override
        public Traversal<Vertex, Long> get_pattern_11_test() {
            return g.V().match(__.as("a").out("KNOWS").as("b"), __.as("a").out("KNOWS").as("c"))
                    .count();
        }

        // fuzzy pattern
        @Override
        public Traversal<Vertex, Long> get_pattern_12_test() {
            return g.V().match(
                            __.as("a").out("KNOWS", "LIKES").as("b"),
                            __.as("b").out("KNOWS", "LIKES").as("c"),
                            __.as("a").out("KNOWS", "LIKES").as("c"))
                    .count();
        }

        // support both
        @Override
        public Traversal<Vertex, Long> get_pattern_13_test() {
            return g.V().match(__.as("a").both("KNOWS").as("b"), __.as("a").both("KNOWS").as("c"))
                    .count();
        }

        // support vertex filter
        @Override
        public Traversal<Vertex, Long> get_pattern_14_test() {
            return g.V().match(
                            __.as("a").out("KNOWS").as("b"),
                            __.as("b").hasLabel("PERSON").has("gender", "male").as("b"),
                            __.as("b").out("KNOWS").as("c"))
                    .count();
        }

        // support project properties
        @Override
        public Traversal<Vertex, Long> get_pattern_15_test() {
            return g.V().match(
                            __.as("a").out("KNOWS").as("b"),
                            __.as("b").out("KNOWS").as("c"),
                            __.as("a").out("KNOWS").as("c"))
                    .select("a", "b", "c")
                    .by("firstName")
                    .by("firstName")
                    .by("firstName")
                    .count();
        }

        // support project properties when project (implicitly) user-given tags after pattern match
        @Override
        public Traversal<Vertex, Long> get_pattern_16_test() {
            return g.V().match(
                            __.as("a").out("KNOWS").out("KNOWS").as("c"),
                            __.as("a").out("KNOWS").as("c"))
                    .select("a", "c")
                    .by("firstName")
                    .by("firstName")
                    .count();
        }

        @Override
        public Traversal<Vertex, Long> get_pattern_17_test() {
            return g.V().match(
                            __.as("a")
                                    .hasLabel("PERSON")
                                    .out("HASINTEREST")
                                    .hasLabel("TAG")
                                    .as("b"),
                            __.as("a")
                                    .hasLabel("PERSON")
                                    .in("HASCREATOR")
                                    .hasLabel("COMMENT", "POST")
                                    .as("c"),
                            __.as("c")
                                    .hasLabel("COMMENT", "POST")
                                    .out("HASTAG")
                                    .hasLabel("TAG")
                                    .as("b"))
                    .count();
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_limit_100_group_test() {
            return g.V().hasLabel("PERSON").limit(100).group().by("id").by(__.count());
        }
    }
}
