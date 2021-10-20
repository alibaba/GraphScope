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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.count;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.valueMap;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

public class GroupOperatorTest extends AbstractOperatorTest {
    public GroupOperatorTest() throws IOException {

    }

    @Test
    public void testGroupCase() {
        GraphTraversal traversal = g.V().out().group();
        executeTreeQuery(traversal);
    }

    @Test
    public void testGroupKeyCase() {
        GraphTraversal traversal = g.V().out().group().by();
        executeTreeQuery(traversal);
    }

    @Test
    public void testGroupTwiceCase() {
        GraphTraversal traversal = g.V().out().group().by().by();
        executeTreeQuery(traversal);
    }

    @Test
    public void testGroupTIdKeyCase() {
        GraphTraversal traversal = g.V().out().group().by(T.id);
        executeTreeQuery(traversal);
    }

    @Test
    public void testGroupTLabelKeyCase() {
        GraphTraversal traversal = g.V().out().group().by(T.label);
        executeTreeQuery(traversal);
    }

    @Test
    public void testGroupNameKeyCase() {
        GraphTraversal traversal = g.V().out().group().by("firstname");
        executeTreeQuery(traversal);
    }

    @Test
    public void testGroupCountKeyCase() {
        GraphTraversal traversal = g.V().out().group().by(__.out().count());
        executeTreeQuery(traversal);
    }

    @Test
    public void testGroupSumKeyCase() {
        GraphTraversal traversal = g.V().out().group().by(__.out().values("id").constant(1d).sum());
        executeTreeQuery(traversal);
    }

    @Test
    public void testGroupTIdCase() {
        GraphTraversal traversal = g.V().out().group().by(__.constant(1)).by(T.id);
        executeTreeQuery(traversal);
    }

    @Test
    public void testGroupTLabelCase() {
        GraphTraversal traversal = g.V().out().group().by(__.constant(1)).by(T.label);
        executeTreeQuery(traversal);
    }

    @Test
    public void testGroupPropCase() {
        GraphTraversal traversal = g.V().out().group().by(__.constant(1)).by("firstname");
        executeTreeQuery(traversal);
    }

    @Test
    public void testGroupConstantMaxCase() {
        executeTreeQuery(g.V().both().hasLabel("person").group().by(constant(1)).by(values("name").max()));
    }

    @Test
    public void testUnfoldAfterGroupCase() {
        GraphTraversal traversal = g.V().out().group().by(__.constant(1)).by("firstname").unfold();
        executeTreeQuery(traversal);
    }

    @Test
    public void testEdgeGroupId() {
        executeTreeQuery(g.V().outE().group().by("id"));
    }

    @Test
    public void testGroupByValueMap() {
        executeTreeQuery(g.V().out().group().by(valueMap("id", "firstname")).by(count()));
    }

    @Test
    public void testGroupByMax() {
        executeTreeQuery(g.V().out().group().by(valueMap("firstname")).by(values("id").max()));
    }

    @Test
    public void testGroupCountProp() {
        executeTreeQuery(g.V().group().by(outE().count()).by("name"));
    }

    @Test
    public void testGroupCountMax() {
        executeTreeQuery(g.V().both().hasLabel("person").group().by(both().count()).by(values("id").max()));
    }

    @Test
    public void testGroupByNameOutWeightSumCase() {
        executeTreeQuery(g.V().hasLabel("person").group().by("name").by(__.outE().values("weight").sum()).order(local).by(Column.values));
    }

    @Test
    public void testGroupValuesOrderCase() {
        executeTreeQuery(g.V().group().by(T.label).by(values("name").order().by(Order.desc).fold()));
    }

    @Test
    public void testGroupLabelOrderLocalCase() {
        executeTreeQuery(g.V().both().group().by(T.label).unfold().select(Column.values).order(local).by("name"));
    }

    @Test
    public void testGroupBySelectKeyCase() {
        executeTreeQuery(g.V().as("a").both().group().by(select("a")).by(count()));
    }

    @Test
    public void testGroupBySelectKeyCase2() {
        executeTreeQuery(g.V().as("a").both().group().by(select("a")).by(count()).unfold());
    }

    @Test
    public void testGroupBySelectLabelCase() {
        executeTreeQuery(g.V().as("a").repeat(__.both()).times(3).emit().values("name").as("b").group().by(__.select("a")).by(__.select("b").dedup().order().fold()));
    }

    @Test
    public void testGroupValueOrderFoldCase() {
        executeTreeQuery(g.V().group().by(T.label).by(__.values("name").order().by(Order.desc).fold()));
    }

    @Test
    public void testGroupByOutCountByNameCase() {
        executeTreeQuery(g.V().hasLabel("person").as("p").out().group().by("name").by(__.select("p").values("age").sum()));
    }
}
