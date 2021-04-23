package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

public class FlatMapOperatorTest extends AbstractOperatorTest {
    public FlatMapOperatorTest() throws IOException {
    }

    @Test
    public void testFlatMapOut() {
        executeTreeQuery(g.V().flatMap(out()).both());
    }

    @Test
    public void testFlatMapOutFold() {
        executeTreeQuery(g.V().flatMap(out().fold()).count());
    }

    @Test
    public void testFlatMap2OutCount() {
        executeTreeQuery(g.V().flatMap(out().out().count()).count());
    }

    @Test
    public void testFlatMapOrderLimit() {
        executeTreeQuery(g.V().hasLabel("person").flatMap(out().order().by("firstName").limit(10)).both().path());
    }

    @Test
    public void testFlatMap2OutPath() {
        executeTreeQuery(g.V().hasLabel("person").flatMap(out().out()).both().path());
    }

//    @Test
//    public void testFlatMapGroupCount() {
//        executeTreeQuery(g.V().flatMap(out().out().groupCount()).path());
//    }
}
