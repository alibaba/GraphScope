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
package com.alibaba.maxgraph.compiler.operator;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;

import java.io.IOException;

public class CostOperatorTest extends AbstractOperatorTest {
    public CostOperatorTest() throws IOException {
    }

    @Test
    public void testCostSelectSimple() {
        executeTreeQuery(g.V("1").as("a").out().select("a").by("name"));
    }

    @Test
    public void testCostSelectTwiceCase() {
        executeTreeQuery(g.V().as("a").both().both().select("a").both().both().select("a").by(__.valueMap()));
    }

    @Test
    public void testCostSelectABTwiceCase() {
        executeTreeQuery(g.V().as("a").both().as("b").both().select("a").both().select("b").both().select("a", "b").by(__.valueMap()));
    }

    @Test
    public void testCostWhereSelectCase() {
        executeTreeQuery(g.V().as("a")
                .out()
                .in().as("b")
                .where("a", P.gt("b")).by("id")
                .select("a", "b").by("name"));
    }

    @Test
    public void testCostSelectThreeLabelCase() {
        executeTreeQuery(g.V().as("a")
                .has("name", "marko").as("b").as("c")
                .select("a", "b", "c")
                .by()
                .by("name")
                .by("id"));
    }
}
