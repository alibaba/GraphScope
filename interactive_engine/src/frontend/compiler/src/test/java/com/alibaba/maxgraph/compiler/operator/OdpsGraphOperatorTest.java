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

import com.alibaba.maxgraph.sdkcommon.compiler.custom.graph.OdpsGraph;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.output.Output;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.program.Program;
import org.junit.Test;

import java.io.IOException;

public class OdpsGraphOperatorTest extends AbstractOperatorTest {

    public OdpsGraphOperatorTest() throws IOException {
    }

    @Test
    public void testOdpsGraphCase() {
        executeTreeQuery(g.fromGraph(
                OdpsGraph.access("1", "123", "http://end123.com")
                        .addEdge("edge1", "proj1", "table1").startType("vertex1").startPrimaryKey("srcid")
                        .endType("vertex2").endPrimaryKey("dstid").direction("out")));
    }

    @Test
    public void testOdpsGraphCCCase() {
        executeTreeQuery(g.fromGraph(
                OdpsGraph.access("1", "123", "http://end123.com")
                        .addEdge("edge1", "proj1", "table1").startType("vertex1").startPrimaryKey("srcid")
                        .endType("vertex2").endPrimaryKey("dstid").direction("out"))
                .program(Program.graphCC().output("id").iteration(10))
                .map(Output.writeOdps()
                        .accessId("id11")
                        .accessKey("key11")
                        .endpoint("http://e11.com")
                        .project("proj")
                        .table("ttable")
                        .ds("tt")
                        .write("id")));
    }
}
