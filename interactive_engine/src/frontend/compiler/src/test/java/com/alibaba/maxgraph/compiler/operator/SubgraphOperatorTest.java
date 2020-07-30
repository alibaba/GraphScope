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

import com.alibaba.maxgraph.sdkcommon.compiler.custom.output.Output;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.program.Program;

import com.alibaba.maxgraph.tinkerpop.traversal.DefaultMaxGraphTraversal;
import org.junit.Test;

import java.io.IOException;

import static com.alibaba.maxgraph.sdkcommon.compiler.custom.program.Program.cc;
import static com.alibaba.maxgraph.sdkcommon.compiler.custom.program.Program.graphCC;

public class SubgraphOperatorTest extends AbstractOperatorTest {

    public SubgraphOperatorTest() throws IOException {
    }

    @Test
    public void testSubgraphCC() {
        executeTreeQuery(
                g.E()
                        .hasLabel("person_knows_person")
                        .subgraph("person_graph").cap("person_graph")
                        .program(Program.graphCC().output("id").iteration(10))
                        .map(Output
                                .writeOdps()
                                .endpoint("http://odps.endpoint")
                                .accessId("odps.id.value")
                                .accessKey("odps.key.value")
                                .project("outProject")
                                .table("outTable")
                                .ds("20181112")
                                .write("firstname", "lastname")));
    }

    @Test
    public void testSubgraphSourceVineyard() {
        DefaultMaxGraphTraversal subgraph = (DefaultMaxGraphTraversal) g.E().hasLabel("person_knows_person").subgraph("person_graph");
        executeTreeQuery(subgraph.outputVineyard("vineyard"));
    }

//    @Test
//    public void testSubgraphRun() {
//        executeTreeQuery(g.E()
//                .hasLabel("person_knows_person")
//                .subgraph("person_graph").cap("person_graph")
//                .program(run("cc.flash"))
//                .map(Output
//                        .writeOdps()
//                        .endpoint("http://odps.endpoint")
//                        .accessId("odps.id.value")
//                        .accessKey("odps.key.value")
//                        .project("outProject")
//                        .table("outTable")
//                        .ds("20181112")
//                        .write("firstname", "lastname")));
//    }

}
