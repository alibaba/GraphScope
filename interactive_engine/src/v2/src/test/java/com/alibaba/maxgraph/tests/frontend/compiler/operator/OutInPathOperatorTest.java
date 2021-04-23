package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;

import java.io.IOException;

public class OutInPathOperatorTest extends AbstractOperatorTest {
    public OutInPathOperatorTest() throws IOException {
    }

    @Test
    public void testTree_V_out_in_out_case() {
        executeTreeQuery(g.V().out().in("person_knows_person").out());
    }

    @Test
    public void testTree_V_out_in_out_path_case() {
        executeTreeQuery(g.V().out().in("person_knows_person").out().path());
    }

    @Test
    public void testTree_V_out_in_out_pathByIdName_case() {
        executeTreeQuery(g.V().out().in("person_knows_person").out().path().by().by(T.id).by("firstname"));
    }
}
