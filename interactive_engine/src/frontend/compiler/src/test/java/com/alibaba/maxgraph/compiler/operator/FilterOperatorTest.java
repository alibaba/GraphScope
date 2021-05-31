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

import com.alibaba.maxgraph.sdkcommon.compiler.custom.Text;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;

public class FilterOperatorTest extends AbstractOperatorTest {
    public FilterOperatorTest() throws Exception {
    }

    @Test
    public void testFilterHasPropCase() {
        executeTreeQuery(g.V().hasLabel("person").has("firstname").out());
    }

    @Test
    public void testFilterNotHasPropCase() {
        executeTreeQuery(g.V().hasLabel("person").not(has("firstname")).out());
    }

    @Test
    public void testFilterOutEInVHasPropCase() {
        executeTreeQuery(g.V().hasLabel("person").has("firstname").outE().has("weight", 1.0).inV().has("firstname", "tom"));
    }

    @Test
    public void testFilterHasPropLimitCase() {
        executeTreeQuery(g.V().hasLabel("person").has("firstname").limit(100));
    }

    @Test
    public void testFilterAndCase() {
        executeTreeQuery(g.V().out().and(has("weight", 1.0), has("firstname", "tom")).in());
    }

    @Test
    public void testFilterOrCase() {
        executeTreeQuery(g.V().out().or(has("weight", 1.0), has("firstname", "tom")).in());
    }

    @Test
    public void testFilterInsideCase() {
        executeTreeQuery(g.V().inE().has("id", P.inside(5, 10)));
    }

    @Test
    public void testFilterHasWithinPropCase() {
        executeTreeQuery(g.V().hasLabel("person").has("id", P.within(1L, 2L, 3L, 4L)).out());
    }

    @Test
    public void testFilterOrCustomTextCase() {
        executeTreeQuery(g.V().both().where(__.or(has("lastname", "tom"),
                has("firstname", Text.startsWith("jack")))).valueMap());
    }

    @Test
    public void testFilterOrSameFieldCustomTextCase() {
        executeTreeQuery(g.V().both().where(__.or(has("firstname", "tom"),
                has("firstname", Text.startsWith("jack")))).valueMap());
    }

    @Test
    public void testFilterAndPredicateTraversalCase() {
        executeTreeQuery(g.V().and(__.has("age", P.gt(27)), __.outE().count().is(P.gte(2L))).values("name"));
    }

    @Test
    public void testFilterAndMultipleSelectCase() {
        executeTreeQuery(g.V().as("a").and(__.select("a"), __.select("a")));
    }

    @Test
    public void testFilterEdgeNotExistLabelCase() {
        executeTreeQuery(g.E().union(hasLabel("aaaa")));
    }
}
