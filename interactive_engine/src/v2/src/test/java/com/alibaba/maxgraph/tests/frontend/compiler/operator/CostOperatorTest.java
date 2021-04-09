package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;

import java.io.IOException;

public class CostOperatorTest extends AbstractOperatorTest {
    public CostOperatorTest() throws IOException {
    }

    @Test
    public void testCostSelectSimple() {
        executeTreeQuery(g.V("1").as("a").out().select("a").by("name"));
    }

    @Test
    public void testCostSelectTwiceCase() {
        executeTreeQuery(g.V().as("a").both().both().select("a").both().both().select("a").by(__.valueMap()));
    }

    @Test
    public void testCostSelectABTwiceCase() {
        executeTreeQuery(g.V().as("a").both().as("b").both().select("a").both().select("b").both().select("a", "b").by(__.valueMap()));
    }

    @Test
    public void testCostWhereSelectCase() {
        executeTreeQuery(g.V().as("a")
                .out()
                .in().as("b")
                .where("a", P.gt("b")).by("id")
                .select("a", "b").by("name"));
    }

    @Test
    public void testCostSelectThreeLabelCase() {
        executeTreeQuery(g.V().as("a")
                .has("name", "marko").as("b").as("c")
                .select("a", "b", "c")
                .by()
                .by("name")
                .by("id"));
    }
}
