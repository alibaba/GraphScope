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

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.fold;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.label;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;

public class RepeatOperatorTest extends AbstractOperatorTest {
    public RepeatOperatorTest() throws IOException {
    }

    @Test
    public void testRepeatOutTimes3() {
        executeTreeQuery(g.V().repeat(out()).times(3).in());
    }

    @Test
    public void testRepeatOutInOutTimes3() {
        executeTreeQuery(g.V().repeat(out().in().out()).times(3).in());
    }

    @Test
    public void testRepeatOutInOutEmitFirstTimes3() {
        executeTreeQuery(g.V().emit().repeat(out().in().out()).times(3).in());
    }

    @Test
    public void testRepeatOutEmitFirstTimes3() {
        executeTreeQuery(g.V().emit().repeat(out()).times(3).in());
    }

    @Test
    public void testRepeatOutInOutEmitOutFirstTimes3() {
        executeTreeQuery(g.V().emit(out()).repeat(out().in().out()).times(3).in());
    }

    @Test
    public void testRepeatOutInOutEmitOutInCountGTFirstTimes3() {
        executeTreeQuery(g.V().emit(out().in().count().is(gt(3))).repeat(out().in().out()).times(3).in());
    }

    @Test
    public void testRepeatOutInOutEmitAfterTimes3() {
        executeTreeQuery(g.V().repeat(out().in().out()).emit().times(3).in());
    }

    @Test
    public void testRepeatOutEmitAfterTimes3() {
        executeTreeQuery(g.V().repeat(out()).emit().times(3).in());
    }

    @Test
    public void testRepeatOutInOutEmitOutAfterTimes3() {
        executeTreeQuery(g.V().repeat(out().in().out()).emit(out()).times(3).in());
    }

    @Test
    public void testRepeatOutInOutEmitOutInCountGTAfterTimes3() {
        executeTreeQuery(g.V().repeat(out().in().out()).emit(out().in().count().is(gt(3))).times(3).in());
    }

    @Test
    public void testRepeatOutUntilOutECountValues() {
        executeTreeQuery(g.V().has("id", 1).repeat(out()).until(outE().count().is(0)).values("name"));
    }

    @Test
    public void testRepeatUntilOrLoopsGt() {
        executeTreeQuery(g.V().has("id", 1).repeat(out()).until(outE().count().is(0).or().loops().is(gt(3))).out().values("name"));
    }

    @Test
    public void testRepeatUntilOrLoopsGt2() {
        executeTreeQuery(g.V().hasLabel("person").has("id", 1)
                .repeat(outE().hasLabel("person_knows_person").otherV().simplePath())
                .until(hasLabel("person").has("id", 1).or().loops().is(gt(2)))
                .hasLabel("person").has("id", 1)
                .path());
    }

    @Test
    public void testRepeatHundun() {
        executeTreeQuery(g
                .V()
                .has("id", "1")
                .repeat(bothE()
                        .has("id", "2")
                        .otherV()
                        .where(
                                label().is(neq("person")))
                        .group()
                        .by(label())
                        .by(
                                fold()
                                        .order(Scope.local).by("name").limit(Scope.local, 20))
                        .select(Column.values)
                        .unfold())
                .times(3)
                .simplePath()
                .path());
    }

    @Test
    public void testRepeatUntilCase() {
        executeTreeQuery(g.V().has("name", "marko").repeat(__.out()).until(__.outE().count().is(0)).values("name"));
    }

    @Test
    public void testRepeatShortestPathCase() {
        executeTreeQuery(g.V().repeat(out().simplePath())
                .until(has("name", "marko")
                        .or().loops().is(gt(5))));
    }

    @Test
    public void testRepeatUntilLimitCase() {
        executeTreeQuery(g.V().repeat(both())
                .until(has("name", "marko")
                        .or().loops().is(gt(5)))
                .limit(1).path());
    }

    @Test
    public void testRepeatLabelCase() {
        executeTreeQuery(g.V().repeat(both()).times(3).as("a").both().as("b").select("a", "b"));
    }

    @Test
    public void testRepeatUntilInCase() {
        executeTreeQuery(g.V().until(__.out().out()).repeat(__.in().in()));
    }
}
