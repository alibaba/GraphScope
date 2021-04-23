package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.apache.tinkerpop.gremlin.structure.Column.values;

public class FoldOperatorTest extends AbstractOperatorTest {

    public FoldOperatorTest() throws IOException {
    }

    @Test
    public void testUnfoldAfterFold() {
        GraphTraversal traversal = g.V().fold().unfold();
        executeTreeQuery(traversal);
    }

    @Test
    public void testUnfoldAfterFoldMap() {
        executeTreeQuery(g.V().hasLabel("person").as("a").group().by().by(__.out().fold()).unfold().where(select(values).count(local).is(P.gt(500l))).count());
    }

    @Test
    public void testUnfoldAfterGroupCount() {
        executeTreeQuery(g.V().out().groupCount().unfold());
    }

    @Test
    public void testUnfoldAfterGroupCountBy() {
        executeTreeQuery(g.V().out().groupCount().by("id").unfold());
    }

    @Test
    public void testUnfoldAfterGroup() {
        executeTreeQuery(g.V().out().group().by("id").by(__.values("age").sum()).unfold());
    }
}
