package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;

import java.io.IOException;

public class ChooseOperatorTest extends AbstractOperatorTest {
    public ChooseOperatorTest() throws IOException {
    }

    @Test
    public void testChooseLabelAndCase() {
        executeTreeQuery(g.V().choose(__.hasLabel("person").and().out(),
                __.out("person_knows_person"),
                __.identity()).values("name"));
    }

    @Test
    public void testChooseLabelAndOutECase() {
        executeTreeQuery(g.V().choose(__.hasLabel("person").and().outE(),
                __.out("person_knows_person"),
                __.identity()).values("name"));
    }
}
