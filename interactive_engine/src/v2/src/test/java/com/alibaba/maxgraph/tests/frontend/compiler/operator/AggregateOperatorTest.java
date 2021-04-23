package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.junit.Test;

import java.io.IOException;

public class AggregateOperatorTest extends AbstractOperatorTest {

    public AggregateOperatorTest() throws IOException {
    }

    @Test
    public void testAggregateOutInCount() {
        executeTreeQuery(g.V().out().group().by().by());
    }

    @Test
    public void testAggregateGroupByList() {
//        executeTreeQuery(g.V().has("name", "marko").repeat(__.bothE().where(P.without("e")).aggregate("e").otherV()).emit().path());
    }
}
