package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
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
