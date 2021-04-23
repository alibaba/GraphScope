package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.junit.Test;

import java.io.IOException;

public class ContextOperatorTest extends AbstractOperatorTest {

    public ContextOperatorTest() throws IOException {
    }

    @Test
    public void testTimeoutCase() {
        executeTreeQuery(g.timeout(60000).V().out().out());
    }

    @Test
    public void testTimeoutSecCase() {
        executeTreeQuery(g.timeoutSec(60).V().out().out());
    }
}
