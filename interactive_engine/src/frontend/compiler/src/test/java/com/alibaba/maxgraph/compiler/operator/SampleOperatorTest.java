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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

public class SampleOperatorTest extends AbstractOperatorTest {
    public SampleOperatorTest() throws IOException {
    }

    @Test
    public void testSampleDefaultCase() {
        executeTreeQuery(g.V().out().sample(1));
    }

    @Test
    public void testSampleRepeatCase() {
        executeTreeQuery(g.V().repeat(out().sample(1)).until(out().count().is(0).or().loops().is(P.gt(2))).path());
    }

    @Test
    public void testSampleECase() {
        executeTreeQuery(g.E().sample(1));
    }

//    @Test
//    public void testSampleByEmptyCase() {
//        executeTreeQuery(g.V().out().sample(2).by());
//    }
//
//    @Test
//    public void testSampleByLengthCase() {
//        executeTreeQuery(g.V().out().sample(2).by("length"));
//    }
//
//    @Test
//    public void testSampleByOutCountCase() {
//        executeTreeQuery(g.V().out().sample(2).by(out().count()));
//    }
//
//    @Test
//    public void testSampleByConstantCase() {
//        executeTreeQuery(g.V().out().sample(2).by(constant(2.0)));
//    }
//
//    @Test
//    public void testSampleByValuesCase() {
//        executeTreeQuery(g.V().out().sample(2).by(values("length")));
//    }
//
//    @Test
//    public void testSampleVoutECase() {
//        executeTreeQuery(g.V().outE().by(sample(2).by("length")));
//    }
}
