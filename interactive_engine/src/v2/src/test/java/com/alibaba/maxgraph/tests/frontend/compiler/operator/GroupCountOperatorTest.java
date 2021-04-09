package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;

public class GroupCountOperatorTest extends AbstractOperatorTest {
    public GroupCountOperatorTest() throws IOException {
    }

    @Test
    public void testGroupCountByCount() {
        GraphTraversal traversal = g.V().<Long>groupCount().by(bothE().count());
        executeTreeQuery(traversal);
    }
}
