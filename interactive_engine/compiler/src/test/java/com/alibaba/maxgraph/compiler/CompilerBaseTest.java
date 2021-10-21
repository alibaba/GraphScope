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
package com.alibaba.maxgraph.compiler;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.cost.statistics.CostDataStatistics;
import com.alibaba.maxgraph.compiler.schema.DefaultGraphSchema;
import com.alibaba.maxgraph.compiler.dfs.DfsTraversal;
import com.alibaba.maxgraph.compiler.optimizer.LogicalPlanOptimizer;
import com.alibaba.maxgraph.compiler.optimizer.OptimizeConfig;
import com.alibaba.maxgraph.compiler.optimizer.QueryFlowBuilder;
import com.alibaba.maxgraph.compiler.graph.DefaultMaxGraph;
import com.alibaba.maxgraph.compiler.optimizer.QueryFlowManager;
import com.alibaba.maxgraph.compiler.schema.DefaultSchemaFetcher;
import com.google.protobuf.TextFormat;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.FileOutputStream;
import java.io.IOException;

public class CompilerBaseTest {
    @Rule public TestName testName = new TestName();

    protected GraphSchema schema;
    private DefaultMaxGraph ldbcGraph;

    protected QueryFlowBuilder queryFlowBuilder = new QueryFlowBuilder();
    protected GraphTraversalSource g;

    protected LogicalPlanOptimizer logicalPlanOptimizer;

    private boolean writeResultPlan =
            Boolean.valueOf(
                    IOUtils.toString(
                            Thread.currentThread()
                                    .getContextClassLoader()
                                    .getResourceAsStream("result/writeFlag"),
                            "utf-8"));

    protected CompilerBaseTest() throws Exception {
        try {
            initLdbcGraph();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        logicalPlanOptimizer = new LogicalPlanOptimizer(new OptimizeConfig(), false, schema, 0);

        g = ldbcGraph.traversal();
    }

    private void initLdbcGraph() throws Exception {
        String schemaValue =
                IOUtils.toString(
                        Thread.currentThread()
                                .getContextClassLoader()
                                .getResourceAsStream("ldbc.schema"),
                        "utf-8");
        schema = DefaultGraphSchema.buildSchemaFromJson(schemaValue);
        CostDataStatistics.initialize(new DefaultSchemaFetcher(schema));
        ldbcGraph = new DefaultMaxGraph();
    }

    protected String getDirectory() {
        throw new RuntimeException();
    }

    protected String getWriteDirectory() {
        throw new RuntimeException();
    }

    protected void assertResultPlan(String resultContent) {
        try {
            if (writeResultPlan) {
                IOUtils.write(
                        resultContent,
                        new FileOutputStream(getWriteDirectory() + "/" + testName.getMethodName()),
                        "UTF-8");
            } else {
                String fileName = getDirectory() + "/" + testName.getMethodName();
                String expectContent =
                        IOUtils.toString(
                                Thread.currentThread()
                                        .getContextClassLoader()
                                        .getResourceAsStream(fileName),
                                "utf-8");
                Assert.assertEquals(
                        StringUtils.trim(expectContent), StringUtils.trim(resultContent));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void executeQuery(GraphTraversal traversal) {
        QueryFlowManager queryFlowManager = logicalPlanOptimizer.build(traversal);
        System.out.println(TextFormat.printToString(queryFlowManager.getQueryFlow().build()));
        assertResultPlan(TextFormat.printToString(queryFlowManager.getQueryFlow().build()));
    }

    protected void executeQuery(DfsTraversal traversal) {
        QueryFlowManager queryFlowManager = logicalPlanOptimizer.build(traversal);
        System.out.println(TextFormat.printToString(queryFlowManager.getQueryFlow().build()));
        assertResultPlan(TextFormat.printToString(queryFlowManager.getQueryFlow().build()));
    }
}
