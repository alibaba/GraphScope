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

package com.alibaba.graphscope.function.test.gremlin;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public abstract class IrGremlinQueryTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Object> get_g_VX4X_bothE_as_otherV();

    public abstract Traversal<Vertex, Object> get_g_V_hasLabel_hasId_values();

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX4X_bothE_as_otherV() {
        Traversal<Vertex, Object> traversal = this.get_g_VX4X_bothE_as_otherV();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected = Arrays.asList("1", "3", "5");

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            ++counter;
        }

        Assert.assertEquals(expected.size(), counter);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_hasLabel_hasId_values() {
        Traversal<Vertex, Object> traversal = this.get_g_V_hasLabel_hasId_values();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "marko";

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    public static class Traversals extends IrGremlinQueryTest {

        @Override
        public Traversal<Vertex, Object> get_g_VX4X_bothE_as_otherV() {
            return g.V().has("id", 4).bothE().as("a").otherV().values("id");
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_hasLabel_hasId_values() {
            return g.V().hasLabel("person").has("id", 1).values("name");
        }
    }
}
