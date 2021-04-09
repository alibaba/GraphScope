package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

public class WhereOperatorTest extends AbstractOperatorTest {
    public WhereOperatorTest() throws Exception {
    }

    @Test
    public void testWhereCountLocalIs() {
        GraphTraversal traversal = g.V().where(values("firstname").count(local).is(gt(4)));
        executeTreeQuery(traversal);
    }

    @Test
    public void testWhereOutCountIs() {
        GraphTraversal traversal = g.V().where(out().count().is(gt(4))).in();
        executeTreeQuery(traversal);
    }

    @Test
    public void testWhereOutInDedupCountIs() {
        GraphTraversal traversal = g.V().where(out().in().dedup().count().is(gt(4))).in();
        executeTreeQuery(traversal);
    }

    @Test
    public void testWherePropByCase() {
        executeTreeQuery(g.V().has("firstname", "marko").as("a").out().has("id").where(P.gt("a")).by("id").values("firstname"));
    }

    @Test
    public void testWhereInOutCountZeroCase() {
        executeTreeQuery(g.V().where(__.in().out().count().is(0)).values("name"));
    }

    @Test
    public void testWhereOrPredicateCase() {
        executeTreeQuery(g.V().where(__.or(has("firstname", "marko"), has("age", gt(20)), has("firstname", "tomcat"))));
    }
}
