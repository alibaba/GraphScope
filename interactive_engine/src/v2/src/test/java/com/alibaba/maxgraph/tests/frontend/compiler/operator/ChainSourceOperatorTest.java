package com.alibaba.maxgraph.tests.frontend.compiler.operator;

import com.alibaba.maxgraph.tests.frontend.compiler.AbstractOperatorTest;
import org.junit.Test;

import java.io.IOException;

public class ChainSourceOperatorTest extends AbstractOperatorTest {
    public ChainSourceOperatorTest() throws IOException {
    }

    @Test
    public void testChainVoutEinVCase() {
        executeTreeQuery(g.V().outE("person_knows_person").has("weight", 1.0).inV().valueMap("firstname"));
    }

    @Test
    public void testChainVLimitoutEinVCase() {
        executeTreeQuery(g.V().limit(3).outE("person_knows_person").has("weight", 1.0).inV().valueMap("firstname"));
    }

    @Test
    public void testChainVoutELimitinVCase() {
        executeTreeQuery(g.V().outE("person_knows_person").has("weight", 1.0).limit(3).inV().valueMap("firstname"));
    }

    @Test
    public void testChainVoutELimitHasinVCase() {
        executeTreeQuery(g.V().outE("person_knows_person").limit(3).has("weight", 1.0).inV().valueMap("firstname"));
    }

    @Test
    public void testChainVAsoutEinVCase() {
        executeTreeQuery(g.V().as("a").outE("person_knows_person").has("weight", 1.0).inV().select("a").valueMap("firstname"));
    }

    @Test
    public void testChainVoutEAsinVCase() {
        executeTreeQuery(g.V().outE("person_knows_person").as("a").has("weight", 1.0).inV().select("a").valueMap("firstname"));
    }

    @Test
    public void testChainVoutEinVAsCase() {
        executeTreeQuery(g.V().outE("person_knows_person").has("weight", 1.0).inV().as("a").out().select("a").valueMap("firstname"));
    }
}
