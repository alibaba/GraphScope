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

import com.alibaba.maxgraph.sdkcommon.compiler.custom.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Test;

import java.io.IOException;

import static com.alibaba.maxgraph.sdkcommon.compiler.custom.aggregate.CustomAggregation.aggregationList;
import static com.alibaba.maxgraph.sdkcommon.compiler.custom.branch.CustomBranch.branchCase;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.count;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.filter;
import static org.apache.tinkerpop.gremlin.structure.Column.keys;
import static org.apache.tinkerpop.gremlin.structure.Column.values;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;

public class CustomBranchOperatorTest extends AbstractOperatorTest {
    public CustomBranchOperatorTest() throws IOException {
    }
//    @Test
//    public void testCustomBranchCaseWhenSimple() {
//        executeTreeQuery(g.V()
//                .fold()
//                .map(branchCase(unfold().values("name").fold())
//                        .when(filter(Lists.contains("josh")))
//                        .then(unfold().constant(1))
//                        .when(filter(Lists.contains("marko")))
//                        .then(unfold().constant(2))
//                        .when(filter(Lists.contains("peter")))
//                        .then(unfold().constant(3))
//                        .elseEnd(constant(4))));
//    }
//
//    @Test
//    public void testCustomBranchCaseWhen() {
//        executeTreeQuery(g.V().as("a")
//                .both().as("b")
//                .both().as("c")
//                .group()
//                .by(select("a", "c"))
//                .by(select("b")
//                        .fold()
//                        .map(branchCase(unfold().values("name").fold())
//                                .when(filter(Lists.contains("josh123")))
//                                .then(unfold().constant(1))
//                                .when(filter(Lists.contains("marko321")))
//                                .then(unfold().constant(2))
//                                .when(filter(Lists.contains("peter21")))
//                                .then(unfold().constant(3))
//                                .elseEnd(unfold().constant(4)))
//                        .sum())
//                .unfold());
//    }
}
