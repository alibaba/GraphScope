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
import java.util.Collection;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;

public class CountOperatorTest extends AbstractOperatorTest {
    public CountOperatorTest() throws IOException {
    }

    @Test
    public void testCountOutCountCase() {
        executeTreeQuery(g.V().out("person_knows_person").count());
    }

    @Test
    public void testCountOutNameCountCase() {
        executeTreeQuery(g.V().out("person_knows_person").has("name", "tom").count());
    }

    @Test
    public void testCountOutECountCase() {
        executeTreeQuery(g.V().outE("person_knows_person").count());
    }

    @Test
    public void testCountVCountCase() {
        executeTreeQuery(g.V().hasLabel("person").count());
    }

    @Test
    public void testCountGroupOutECountCase() {
        executeTreeQuery(g.V().<Long, Collection<String>>group().by(outE().count()).by("name"));
    }

    @Test
    public void testCountAfterLimitCase() {
        executeTreeQuery(g.V().outE().limit(10000).count());
    }

    @Test
    public void testCountEstimateVCase() {
        executeTreeQuery(g.estimateVCount());
    }

    @Test
    public void testCountEstimateVPersonCase() {
        executeTreeQuery(g.estimateVCount("person"));
    }

    @Test
    public void testCountEstimateECase() {
        executeTreeQuery(g.estimateECount());
    }

    @Test
    public void testCountEstimateEKnowsCase() {
        executeTreeQuery(g.estimateECount("person_knows_person"));
    }
}
