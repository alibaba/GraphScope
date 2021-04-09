package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.label;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent.Pick.any;
import static org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent.Pick.none;

public class BranchOperatorTest extends AbstractOperatorTest {
    public BranchOperatorTest() throws IOException {
    }

    @Test
    public void testBranchSimpleCase() {
        executeTreeQuery(g.V()
                .branch(values("firstname"))
                .option("tom", out())
                .option("jim", values("birthday"))
                .option(none, in()));
    }

    @Test
    public void testBranchAnyCase() {
        executeTreeQuery(g.V()
                .branch(values("firstname"))
                .option(any, both())
                .option("tom", out())
                .option(none, in())
                .option("jim", values("birthday")));
    }

    @Test
    public void testBranchCountCase() {
        executeTreeQuery(g.V()
                .branch(label().is("person").count())
                .option(1L, values("age"))
                .option(0L, values("lang"))
                .option(0L, values("lang")));
    }

    @Test
    public void testBranchCountAnyCase() {
        executeTreeQuery(g.V()
                .branch(label().is("person").count())
                .option(1L, values("age"))
                .option(0L, values("lang"))
                .option(0L, values("lang"))
                .option(any, label()));
    }

    @Test
    public void testBranchLabelCase() {
        executeTreeQuery(g.V()
                .branch(label())
                .option("person", values("age"))
                .option("place", out()));
    }
}
