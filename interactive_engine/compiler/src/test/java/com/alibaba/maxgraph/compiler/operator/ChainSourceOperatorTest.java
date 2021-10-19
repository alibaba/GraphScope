/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.operator;

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
