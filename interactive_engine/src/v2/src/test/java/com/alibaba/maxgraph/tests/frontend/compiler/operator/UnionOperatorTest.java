package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.is;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

public class UnionOperatorTest extends AbstractOperatorTest {
    public UnionOperatorTest() throws IOException {
    }

    @Test
    public void testUnionOrderByAge() {
        executeTreeQuery(g.V()
                .has("age")
                .order()
                .by(
                        values("age")
                                .union(
                                        is(P.gt(29)).constant(1),
                                        is(P.eq(29)).constant(2),
                                        is(P.lt(29)).constant(3))
                ));
    }

    @Test
    public void testUnionValueAgeOrderBy() {
        executeTreeQuery(g.V()
                .has("age")
                .as("a")
                .values("age")
                .union(
                        is(P.gt(29)).constant(1),
                        is(P.eq(29)).constant(2),
                        is(P.lt(29)).constant(3)
                ).as("b")
                .select("a")
                .order().by(select("b")));
    }
}
