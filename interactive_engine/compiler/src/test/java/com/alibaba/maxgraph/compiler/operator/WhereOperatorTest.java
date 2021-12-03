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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

public class WhereOperatorTest extends AbstractOperatorTest {
    public WhereOperatorTest() throws Exception {
    }

    @Test
    public void testWhereCountLocalIs() {
        GraphTraversal traversal = g.V().where(values("firstname").count(local).is(gt(4)));
        executeTreeQuery(traversal);
    }

    @Test
    public void testWhereOutCountIs() {
        GraphTraversal traversal = g.V().where(out().count().is(gt(4))).in();
        executeTreeQuery(traversal);
    }

    @Test
    public void testWhereOutInDedupCountIs() {
        GraphTraversal traversal = g.V().where(out().in().dedup().count().is(gt(4))).in();
        executeTreeQuery(traversal);
    }

    @Test
    public void testWherePropByCase() {
        executeTreeQuery(g.V().has("firstname", "marko").as("a").out().has("id").where(P.gt("a")).by("id").values("firstname"));
    }

    @Test
    public void testWhereInOutCountZeroCase() {
        executeTreeQuery(g.V().where(__.in().out().count().is(0)).values("name"));
    }

    @Test
    public void testWhereOrPredicateCase() {
        executeTreeQuery(g.V().where(__.or(has("firstname", "marko"), has("age", gt(20)), has("firstname", "tomcat"))));
    }
}
