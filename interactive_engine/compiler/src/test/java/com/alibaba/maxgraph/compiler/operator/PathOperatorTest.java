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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

public class PathOperatorTest extends AbstractOperatorTest {
    public PathOperatorTest() throws Exception {
    }

    @Test
    public void testPath_V_path_case() {
        GraphTraversal traversal = g.V().path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testPath_V_out_in_path_case() {
        GraphTraversal traversal = g.V().out().in().path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testPath_V_out_in_pathByFirstnameByLastname_case() {
        GraphTraversal traversal = g.V().out().in().path().by("firstname").by("lastname");
        executeTreeQuery(traversal);
    }

    @Test
    public void testPath_V_has_out_filter_in_prop_pathById_case() {
        GraphTraversal traversal = g.V().has("firstname", "tom")
                .out().filter(values("firstname").is(eq("tom2")))
                .in()
                .values("firstname", "lastname")
                .path().by(T.id);
        executeTreeQuery(traversal);
    }

    @Test
    public void testPath_V_has_out_filter_in_valueMap_path_case() {
        GraphTraversal traversal = g.V().has("firstname", "tom")
                .out().filter(values("firstname").is(eq("tom2")))
                .in()
                .valueMap("firstname", "lastname")
                .path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testPath_V_has_out_filter_in_properties_path_case() {
        GraphTraversal traversal = g.V().has("firstname", "tom")
                .out().filter(values("firstname").is(eq("tom2")))
                .in()
                .properties("firstname", "lastname")
                .path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testPath_V_has_out_where_in_properties_path_case() {
        GraphTraversal traversal = g.V().has("firstname", "tom")
                .out().where(values("firstname").is(eq("tom2")))
                .in()
                .properties("firstname", "lastname")
                .path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testPath_V_has_out_whereOutIn_in_properties_path_case() {
        GraphTraversal traversal = g.V().has("firstname", "tom")
                .out().where(out().in())
                .in()
                .properties("firstname", "lastname")
                .path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testPath_V_UnionOutIn_valueMap_path_case() {
        GraphTraversal traversal = g.V().union(out().in(), in().out())
                .valueMap("firstname", "lastname")
                .path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testPath_V_UnionOutHasIn_valueMap_path_case() {
        GraphTraversal traversal = g.V().union(out().has("firstname", "tom").in().out().in(), in().out())
                .valueMap("firstname", "lastname")
                .path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testPath_V_UnionOutFilterIn_valueMap_path_case() {
        GraphTraversal traversal = g.V().union(out().filter(values("firstname").is(eq("tom"))).in().out().in(), in().out())
                .valueMap("firstname", "lastname")
                .path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testPath_V_UnionOutFilterInThree_valueMap_path_case() {
        GraphTraversal traversal = g.V()
                .union(
                        out().filter(values("firstname").is(eq("tom"))).in().out().in(),
                        in().out(),
                        out().both().in())
                .valueMap("firstname", "lastname")
                .path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testPath_V_outE_order_range_inV_path_case() {
        GraphTraversal traversal = g.V()
                .outE()
                .order().by("weight")
                .inV()
                .range(10, 20)
                .path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testPath_V_out_groupCount_unfold_path_case() {
        GraphTraversal traversal = g.V()
                .out()
                .groupCount()
                .unfold()
                .path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testPath_V_out_groupByNameCount_unfold_path_case() {
        GraphTraversal traversal = g.V()
                .out()
                .group().by("firstname").by(out().count())
                .unfold()
                .path();
        executeTreeQuery(traversal);

    }

    @Test
    public void testPath_V_out_valueAge_max_path_case() {
        GraphTraversal traversal = g.V()
                .out()
                .values("id")
                .max()
                .path();
        executeTreeQuery(traversal);

    }

    @Test
    public void testPathRepeatUntilCase() {
        executeTreeQuery(g.V().repeat(both().simplePath()).until(has("name", "peter").or().loops().is(2)).has("name", "peter").path().by("name"));
    }

    @Test
    public void testPathLabelSelectKeysCase() {
        executeTreeQuery(g.V().as("a", "b").out().as("c").path().select(Column.keys));
    }
}
