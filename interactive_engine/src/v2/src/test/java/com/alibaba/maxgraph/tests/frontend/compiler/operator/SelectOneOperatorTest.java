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
package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.count;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.valueMap;

public class SelectOneOperatorTest extends AbstractOperatorTest {
    public SelectOneOperatorTest() throws IOException {
    }

    @Test
    public void testTree_V_as_out_select_case() {
        executeTreeQuery(g.V().as("a").out().select("a"));
    }

    @Test
    public void testTree_V_as_out_selectByTid_case() {
        executeTreeQuery(g.V().as("a").out().select("a").by(T.id));
    }

    @Test
    public void testTree_V_as_out_selectByEmpty_case() {
        executeTreeQuery(g.V().as("a").out().select("a").by());
    }

    @Test
    public void testTree_V_as_out_selectByName_case() {
        executeTreeQuery(g.V().as("a").out().select("a").by("firstname"));
    }

    @Test
    public void testTree_V_as_out_selectByCount_case() {
        executeTreeQuery(g.V().as("a").out().select("a").by(count()));
    }

    @Test
    public void testTree_V_as_out_selectByOutInCount_case() {
        executeTreeQuery(g.V().as("a").out().select("a").by(out().in().count()));
    }

    @Test
    public void testTree_V_as_out_selectByOutFold_case() {
        executeTreeQuery(g.V().as("a").out().select("a").by(out().fold()));
    }

    @Test
    public void testTree_V_as_out_selectByOutFoldCountLocal_case() {
        executeTreeQuery(g.V().as("a").out().select("a").by(out().fold().count(Scope.local)));
    }

    @Test
    public void testTree_V_as_out_selectByOutIn_case() {
        executeTreeQuery(g.V().as("a").out().select("a").by(out().in()));
    }

    @Test
    public void testSelectCase() {
        executeTreeQuery(g.V().as("a").out().as("b").select("a", "b"));
    }

    @Test
    public void testSelectCaseByNameId() {
        executeTreeQuery(g.V().as("a").out().as("b").select("a", "b").by("name").by("id"));
    }

    @Test
    public void testSelectCaseByNameOutCount() {
        executeTreeQuery(g.V().as("a").out().as("b").select("a", "b").by(valueMap()).by(out().count()));
    }

    @Test
    public void testSelectFilterCaseByNameOutCount() {
        executeTreeQuery(g.V().as("a")
                .out().as("b")
                .has("name", "Tom")
                .has("id", 123)
                .select("a", "b")
                .by(valueMap()).by(out().count()));
    }

    @Test
    public void testSelectOutFilterCaseByNameOutCount() {
        executeTreeQuery(g.V().as("a")
                .out().as("b")
                .out()
                .has("name", "Tom")
                .has("id", 123)
                .select("a", "b")
                .by(valueMap()).by(out().count()));
    }

    @Test
    public void testSelectMultiSubQueryCase() {
        executeTreeQuery(g.V().hasLabel("person").as("name").as("language").as("creators").select("name", "language", "creators").by("name").by("id").by(__.in().values("name").fold().order(Scope.local)));
    }

    @Test
    public void testSelectAfterRepeatCase() {
        executeTreeQuery(g.V().until(__.out().out()).repeat(__.in().as("a").in().as("b")).select("a", "b").by("name"));
    }
}
