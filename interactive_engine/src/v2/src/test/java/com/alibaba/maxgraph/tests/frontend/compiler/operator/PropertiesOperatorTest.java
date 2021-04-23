package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.junit.Test;

import java.io.IOException;

public class PropertiesOperatorTest extends AbstractOperatorTest {

    public PropertiesOperatorTest() throws IOException {
    }

    @Test
    public void testPropertiesCase() {
        GraphTraversal traversal = g.V().properties("firstname", "lastname");
        executeTreeQuery(traversal);
    }
}
