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

package com.alibaba.graphscope.integration.pattern;

import com.alibaba.graphscope.gremlin.antlr4.__;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversal;

import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

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

    @Test
    public void run_pattern_1_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_1_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "343476";
        while (traversal.hasNext()) {
            Long bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    @Test
    public void run_pattern_2_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_2_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "87328";
        while (traversal.hasNext()) {
            Long bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    @Test
    public void run_pattern_3_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_3_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "1547850";
        while (traversal.hasNext()) {
            Long bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    @Test
    public void run_pattern_4_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_4_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "33380";
        while (traversal.hasNext()) {
            Long bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    @Test
    public void run_pattern_5_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_5_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "31580";
        while (traversal.hasNext()) {
            Long bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    @Test
    public void run_pattern_6_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_6_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "71733";
        while (traversal.hasNext()) {
            Long bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    @Test
    public void run_pattern_7_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_7_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "6568";
        while (traversal.hasNext()) {
            Long bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    @Test
    public void run_pattern_8_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_8_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "1594426";
        while (traversal.hasNext()) {
            Long bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    @Test
    public void run_pattern_9_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_9_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "33380";
        while (traversal.hasNext()) {
            Long bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    @Test
    public void run_pattern_10_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_10_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "7327";
        while (traversal.hasNext()) {
            Long bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    @Test
    public void run_pattern_11_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_11_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "24738776";
        while (traversal.hasNext()) {
            Long bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    @Test
    public void run_pattern_12_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_12_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "89621";
        while (traversal.hasNext()) {
            Long bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    @Test
    public void run_pattern_13_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_13_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "2430116";
        while (traversal.hasNext()) {
            Long bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    @Test
    public void run_pattern_14_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_14_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "87328";
        while (traversal.hasNext()) {
            Long bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    @Test
    public void run_pattern_15_test() {
        Traversal<Vertex, Long> traversal = this.get_pattern_15_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "33380";
        while (traversal.hasNext()) {
            Long bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    public static class Traversals extends PatternQueryTest {

        // PM1
        @Override
        public Traversal<Vertex, Long> get_pattern_1_test() {
            return g.V().match(
                            __.as("a").out("KNOWS").as("b"),
                            __.as("b")
                                    .outE("KNOWS")
                                    .has("creationDate", P.gt(20120101000000000L))
                                    .inV()
                                    .as("c"))
                    .count();
        }

        // PM2
        @Override
        public Traversal<Vertex, Long> get_pattern_2_test() {
            return g.V().match(
                            __.as("a").out("KNOWS").as("b"),
                            __.as("b")
                                    .hasLabel("PERSON")
                                    .has("creationDate", P.gt(20120101000000000L))
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
                                    .has("creationDate", P.gt(20120101000000000L))
                                    .outE("KNOWS")
                                    .has("creationDate", P.gt(20120601000000000L))
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
                            __.as("a")
                                    .outE("KNOWS")
                                    .has("creationDate", P.gt(20110101000000000L))
                                    .inV()
                                    .as("b"),
                            __.as("b")
                                    .outE("KNOWS")
                                    .has("creationDate", P.gt(20110101000000000L))
                                    .inV()
                                    .as("c"),
                            __.as("a")
                                    .outE("KNOWS")
                                    .has("creationDate", P.gt(20110101000000000L))
                                    .inV()
                                    .as("c"))
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
                            __.as("b")
                                    .has("creationDate", P.gt(20120101000000000L))
                                    .outE("KNOWS")
                                    .has("creationDate", P.gt(20120201000000000L))
                                    .inV()
                                    .as("c"),
                            __.as("a").out("KNOWS").as("c"),
                            __.as("c").out("KNOWS").as("d"),
                            __.as("b")
                                    .outE("KNOWS")
                                    .has("creationDate", P.gt(20120301000000000L))
                                    .inV()
                                    .as("d"))
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
                                                    .has("creationDate", P.gt(20120101000000000L))
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
                                            __.as("b")
                                                    .has("creationDate", P.gt(20120101000000000L))
                                                    .outE("KNOWS")
                                                    .has("creationDate", P.gt(20120601000000000L)))
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
            return g.V().match(__.as("a").out().as("b"), __.as("a").out().as("c")).count();
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
                            __.as("b")
                                    .hasLabel("PERSON")
                                    .has("creationDate", P.gt(20120101000000000L))
                                    .as("b"),
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
    }
}
