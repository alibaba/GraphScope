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
import com.alibaba.maxgraph.sdkcommon.compiler.custom.dim.Dim;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.output.Output;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;

public class DimOutputOperatorTest extends AbstractOperatorTest {
    public DimOutputOperatorTest() throws Exception {
    }

    @Test
    public void testDimOdpsCase() {
        GraphTraversal traversal = g.V()
                .hasLabel("person")
                .has("id", Dim.within(
                        Dim.loadOdps()
                                .endpoint("http://odps.endpoint")
                                .accessId("odps.id.value")
                                .accessKey("odps.key.value")
                                .project("projectName")
                                .table("tableName")
                                .ds("20181112")
                                .has("firstname", P.eq("tom1"))
                                .has("lastname", P.eq("tom2"))
                                .pklist("id1", "id2")))
                .has("birthday", P.eq("2014-06-08"))
                .out()
                .in();
        executeTreeQuery(traversal);
    }

    @Test
    public void testDimOdpsIdCase() {
        GraphTraversal traversal = g.V()
                .hasLabel("person")
                .hasId(Dim.within(
                        Dim.loadOdps()
                                .endpoint("http://odps.endpoint")
                                .accessId("odps.id.value")
                                .accessKey("odps.key.value")
                                .project("projectName")
                                .table("tableName")
                                .ds("20181112")
                                .has("firstname", P.eq("tom1"))
                                .has("lastname", P.eq("tom2"))
                                .pklist("id1", "id2")))
                .has("birthday", P.eq("2014-06-08"))
                .out()
                .in();
        executeTreeQuery(traversal);
    }

    @Test
    public void testDimOdpsOutputOdpsCase() {
        GraphTraversal traversal = g.V()
                .hasLabel("person")
                .hasId(Dim.within(
                        Dim.loadOdps()
                                .endpoint("http://odps.endpoint")
                                .accessId("odps.id.value")
                                .accessKey("odps.key.value")
                                .project("projectName")
                                .table("tableName")
                                .ds("20181112")
                                .has("firstname", P.eq("tom1"))
                                .has("lastname", P.eq("tom2"))
                                .pklist("id1", "id2")))
                .has("birthday", P.eq("2014-06-08"))
                .out()
                .in()
                .map(Output
                        .writeOdps()
                        .endpoint("http://odps.endpoint")
                        .accessId("odps.id.value")
                        .accessKey("odps.key.value")
                        .project("outProject")
                        .table("outTable")
                        .ds("20181112")
                        .write("firstname", "lastname"));
        executeTreeQuery(traversal);

    }

    @Test
    public void testQianMoCase() {
        GraphTraversal traversal = g.V().hasLabel("person").hasId(
            Dim.within(Dim.loadOdps().endpoint("http://").project("").accessId("").accessKey("").table("concept").ds("").pklist("cid")
                .has("channel", P.eq(12)).has("prob", P.gt(0.6))))
            .where(__.out("person_knows_person").count().is(P.gt(3))).as("a")
            .out().has("age", Lists.contains(5)).group().by().by(select("a").count())
            .unfold().where(select(Column.values).count(local).is(P.gt(200)))
            .map(Output.writeOdps().endpoint("http://").accessId("").accessKey("").project("").table("concept2item").write());
        executeTreeQuery(traversal);

    }
}
