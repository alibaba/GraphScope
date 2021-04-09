package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
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
