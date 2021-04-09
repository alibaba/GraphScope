package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.junit.Test;

public class EdgeOperatorTest extends AbstractOperatorTest {
    public EdgeOperatorTest() throws Exception {
    }

    @Test
    public void testEdgeCase() {
        executeTreeQuery(g.E().inV());
    }

    @Test
    public void testEdgeLabelIdCase() {
        executeTreeQuery(g.E(1L).hasLabel("person_knows_person").outV());
    }

    @Test
    public void testEdgeWeightFilterCase() {
        executeTreeQuery(g.E().hasLabel("person_knows_person").has("weight", 1.0).outV());
    }

    @Test
    public void testEdgeValueMapCase() {
        executeTreeQuery(g.E().valueMap());
    }
}
