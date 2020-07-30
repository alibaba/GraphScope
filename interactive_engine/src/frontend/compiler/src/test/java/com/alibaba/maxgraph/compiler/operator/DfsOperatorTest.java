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

import com.alibaba.maxgraph.common.cluster.MaxGraphConfiguration;
import com.alibaba.maxgraph.compiler.dfs.DefaultGraphDfs;
import com.alibaba.maxgraph.compiler.dfs.DfsTraversal;
import com.alibaba.maxgraph.compiler.optimizer.QueryFlowManager;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.google.protobuf.TextFormat;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class DfsOperatorTest extends AbstractOperatorTest {
    private TinkerMaxGraph tinkerMaxGraph;

    public DfsOperatorTest() throws IOException {
        tinkerMaxGraph = new TinkerMaxGraph(new MaxGraphConfiguration(), null,  new DefaultGraphDfs());
    }

    @Test
    public void testDfsSimpleCase() {
        Object obj = tinkerMaxGraph.dfs(g.enableDebugLog().V().out().out(), 10, 20, 1);
        QueryFlowManager queryFlowManager = super.logicalPlanOptimizer.build((DfsTraversal) obj);
        String resultContent = TextFormat.printToString(queryFlowManager.getQueryFlow().build());
        System.out.println(resultContent);
        if (super.writeFlag) {
            try {
                IOUtils.write(resultContent, new FileOutputStream(new File(getWriteDirectory() + "/" + name.getMethodName())), "utf-8");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            assertResultPlan(name.getMethodName(), resultContent);
        }
    }
}
