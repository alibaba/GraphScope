package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.P.within;

public class StoreOperatorTest extends AbstractOperatorTest {
    public StoreOperatorTest() throws IOException {
    }

    @Test
    public void testStoreWhereWithinCase() {
        GraphTraversal traversal = g.V().out().dedup().store("list").out().where(within("list")).valueMap();
        executeTreeQuery(traversal);
    }
}
