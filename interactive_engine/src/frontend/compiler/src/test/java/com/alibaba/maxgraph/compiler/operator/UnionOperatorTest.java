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
