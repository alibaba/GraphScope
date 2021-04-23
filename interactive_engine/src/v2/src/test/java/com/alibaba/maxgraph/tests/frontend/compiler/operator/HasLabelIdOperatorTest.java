package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.junit.Test;

import java.util.Collections;

public class HasLabelIdOperatorTest extends AbstractOperatorTest {
    public HasLabelIdOperatorTest() throws Exception {
    }

    @Test
    public void testHasSingleLabel() {
        GraphTraversal traversal = g.V().hasLabel("person").has("id", 123).out();
        executeTreeQuery(traversal);
    }

    @Test
    public void testHasMultipleLabel() {
        GraphTraversal traversal = g.V().hasLabel("person", "post", "organisation").out();
        executeTreeQuery(traversal);
    }

    @Test
    public void testHasMultipleLabelWithId() {
        GraphTraversal traversal = g.V().hasLabel("person", "post", "organisation").has("id", 123).out();
        executeTreeQuery(traversal);
    }

    @Test
    public void testHasSingleId() {
        GraphTraversal traversal = g.V().hasId(1).out();
        executeTreeQuery(traversal);
    }

    @Test
    public void testHasMultipleId() {
        GraphTraversal traversal = g.V().hasId(1, 2, 3).out();
        executeTreeQuery(traversal);
    }

    @Test
    public void testHasLabelIdMixed() {
        GraphTraversal traversal = g.V()
                .hasLabel("person", "post", "organisation")
                .hasId(1, 2, 3).out();
        executeTreeQuery(traversal);
    }

    @Test
    public void testHasIdEmptiyCase() {
        executeTreeQuery(g.V().hasId(Collections.emptyList()).count());
    }

    @Test
    public void testHasIdWithinEmptyCase() {
        executeTreeQuery(g.V().hasId(P.within(Collections.emptyList())).count());
    }

    @Test
    public void testHasPropKeyValueCase() {
        executeTreeQuery(g.V().both().properties().dedup().hasKey("age").hasValue(P.gt(30)).value());
    }
}
