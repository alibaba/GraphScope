package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

public class SampleOperatorTest extends AbstractOperatorTest {
    public SampleOperatorTest() throws IOException {
    }

    @Test
    public void testSampleDefaultCase() {
        executeTreeQuery(g.V().out().sample(1));
    }

    @Test
    public void testSampleECase() {
        executeTreeQuery(g.E().sample(1));
    }

}
