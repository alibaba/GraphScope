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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.apache.tinkerpop.gremlin.structure.Column.values;

public class FoldOperatorTest extends AbstractOperatorTest {

    public FoldOperatorTest() throws IOException {
    }

    @Test
    public void testUnfoldAfterFold() {
        GraphTraversal traversal = g.V().fold().unfold();
        executeTreeQuery(traversal);
    }

    @Test
    public void testUnfoldAfterFoldMap() {
        executeTreeQuery(g.V().hasLabel("person").as("a").group().by().by(__.out().fold()).unfold().where(select(values).count(local).is(P.gt(500l))).count());
    }

    @Test
    public void testUnfoldAfterGroupCount() {
        executeTreeQuery(g.V().out().groupCount().unfold());
    }

    @Test
    public void testUnfoldAfterGroupCountBy() {
        executeTreeQuery(g.V().out().groupCount().by("id").unfold());
    }

    @Test
    public void testUnfoldAfterGroup() {
        executeTreeQuery(g.V().out().group().by("id").by(__.values("age").sum()).unfold());
    }
}
