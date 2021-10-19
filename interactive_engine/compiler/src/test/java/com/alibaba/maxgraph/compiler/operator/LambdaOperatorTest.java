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

import com.alibaba.maxgraph.result.VertexResult;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

public class LambdaOperatorTest extends AbstractOperatorTest {
    public LambdaOperatorTest() throws IOException {
    }

    @Test
    public void testLambdaFilterSimple(){
        executeTreeQuery(g.V().filter(t -> t.get().values("name").next().equals("marko")));
    }

    @Test
    public void testLambdaFilterWithWhere(){
        executeTreeQuery(g.V().where(out().filter(t -> t.get().values("name").next().equals("marko"))));
    }

    @Test
    public void testLambdaFilterWithRepeat(){
        executeTreeQuery(g.V().repeat(out().filter(t -> (Integer)t.get().values("age").next()>27)).times(2));
    }

    @Test
    public void testLambdaFilterWithUntil() {
        executeTreeQuery(g.V().until(t->t.get().values("name").next().equals("marko")).repeat(out()));
    }

    @Test
    public void testLambdaMapSimple() {
        executeTreeQuery(g.V().map(t->t.get().values("name")));
    }

    @Test
    public void testLambdaFlatMapSimple() {
        executeTreeQuery(g.V().flatMap(t-> ((VertexResult)t.get()).self()));
    }

    @Test
    public void testLambdaCombination(){
        executeTreeQuery(g.V()
                .filter(t -> t.get().values("name").next().equals("marko"))
                .flatMap(t -> ((VertexResult)t.get()).self())
                .map(t -> t.get().value("name")).valueMap());
    }

}
