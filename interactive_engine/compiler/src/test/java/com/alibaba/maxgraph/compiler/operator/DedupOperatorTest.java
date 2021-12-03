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

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;

public class DedupOperatorTest extends AbstractOperatorTest {

    public DedupOperatorTest() throws IOException {
    }

    @Test
    public void testDedupByPropCase() {
        executeTreeQuery(g.V().both().has(T.label, "person").dedup().by("firstname").values("lastname"));
    }

    @Test
    public void testDedupByOutCountCase() {
        executeTreeQuery(g.V().both().both().dedup().by(outE().count()).values("name"));
    }

    @Test
    public void testDedupAfterSelectCase() {
        executeTreeQuery(g.V().as("a").both().as("b").both().select("a", "b").dedup());
    }

    @Test
    public void testDedupBothCase() {
        executeTreeQuery(g.V().both().both().dedup());
    }

    @Test
    public void testDedupWherePredicateCase() {
        executeTreeQuery(g.V().as("a").both().both().where(neq("a")).dedup());
    }
}
