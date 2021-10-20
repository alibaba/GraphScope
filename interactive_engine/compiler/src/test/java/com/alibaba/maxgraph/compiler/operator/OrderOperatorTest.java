/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.operator;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lt;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.is;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

public class OrderOperatorTest extends AbstractOperatorTest {

    public OrderOperatorTest() throws IOException {}

    @Test
    public void testOrderVertexCase() {
        GraphTraversal traversal = g.V().order().by("firstname");
        executeTreeQuery(traversal);
    }

    @Test
    public void testOrderEdgeCase() {
        GraphTraversal traversal = g.V().outE().order().by("weight");
        executeTreeQuery(traversal);
    }

    @Test
    public void testOrderVertexPropCase() {
        GraphTraversal traversal = g.V().order().by("firstname").values("firstname", "birthday");
        executeTreeQuery(traversal);
    }

    @Test
    public void testOrderEdgePropCase() {
        GraphTraversal traversal = g.V().outE().order().by("weight").values("creationdate");
        executeTreeQuery(traversal);
    }

    @Test
    public void testOrderVertexInOutPropCase() {
        GraphTraversal traversal =
                g.V().order().by("firstname").in().out().values("lastname", "birthday");
        executeTreeQuery(traversal);
    }

    @Test
    public void testOrderEdgeInVPropCase() {
        GraphTraversal traversal = g.V().outE().order().by("weight").inV().values("lastname");
        executeTreeQuery(traversal);
    }

    @Test
    public void testOrderEdgeInVPropCountCase() {
        GraphTraversal traversal =
                g.V().outE().order().by("weight").inV().values("lastname").count();
        executeTreeQuery(traversal);
    }

    @Test
    public void testOrderEdgeInVPropFoldCase() {
        GraphTraversal traversal =
                g.V().outE().order().by("weight").inV().values("lastname").fold().count();
        executeTreeQuery(traversal);
    }

    @Test
    public void testOrderVFilterRangeCase() {
        executeTreeQuery(g.V().hasLabel("person").has("firstname", "tom").order().range(100, 200));
    }

    @Test
    public void testOrderByUnionCase() {
        executeTreeQuery(
                g.V().hasLabel("person")
                        .order()
                        .by(
                                values("age")
                                        .union(
                                                is(gt(29)).constant(1),
                                                is(eq(29)).constant(2),
                                                is(lt(29)).constant(3))));
    }

    @Test
    public void testOrderByTwoPropCase() {
        executeTreeQuery(g.V().hasLabel("person").order().by("firstname").by("lastname"));
    }

    @Test
    public void testOrderByTwoAggCase() {
        executeTreeQuery(
                g.V().hasLabel("person").order().by(out().count()).by(values("firstname").max()));
    }

    @Test
    public void testOrderByTwoCountCase() {
        executeTreeQuery(g.V().hasLabel("person").order().by(out().count()).by(both().count()));
    }

    @Test
    public void testOrderLocalByNameCase() {
        executeTreeQuery(
                g.V().both()
                        .group()
                        .by(T.label)
                        .unfold()
                        .select(Column.values)
                        .order(local)
                        .by("name"));
    }

    @Test
    public void testOrderLocalMapCase() {
        executeTreeQuery(g.V().both().groupCount().order(local).by(Column.values));
    }

    @Test
    public void testOrderOutCountDescCase() {
        executeTreeQuery(g.V().order().by(__.outE().count(), desc));
    }
}
