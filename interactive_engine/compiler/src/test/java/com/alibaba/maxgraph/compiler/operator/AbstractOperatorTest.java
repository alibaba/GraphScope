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

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.cost.statistics.CostDataStatistics;
import com.alibaba.maxgraph.compiler.schema.DefaultGraphSchema;
import com.alibaba.maxgraph.compiler.schema.DefaultSchemaFetcher;
import com.alibaba.maxgraph.sdkcommon.exception.MetaException;
import com.alibaba.maxgraph.compiler.graph.DefaultMaxGraph;
import com.alibaba.maxgraph.compiler.optimizer.CompilerConfig;
import com.alibaba.maxgraph.compiler.optimizer.LogicalPlanOptimizer;
import com.alibaba.maxgraph.compiler.optimizer.OptimizeConfig;
import com.alibaba.maxgraph.compiler.optimizer.QueryFlowBuilder;
import com.alibaba.maxgraph.compiler.optimizer.QueryFlowManager;
import com.alibaba.maxgraph.tinkerpop.traversal.MaxGraphTraversalSource;
import com.google.protobuf.TextFormat;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class AbstractOperatorTest {
    @Rule public TestName name = new TestName();

    protected GraphSchema schema;
    private DefaultMaxGraph ldbcGraph;

    protected QueryFlowBuilder queryFlowBuilder = new QueryFlowBuilder();
    protected MaxGraphTraversalSource g;

    protected OptimizeConfig optimizeConfig;
    protected CompilerConfig compilerConfig;

    protected LogicalPlanOptimizer logicalPlanOptimizer;

    protected boolean writeFlag =
            Boolean.valueOf(
                    IOUtils.toString(
                            Thread.currentThread()
                                    .getContextClassLoader()
                                    .getResourceAsStream("result/writeFlag"),
                            "utf-8"));

    protected AbstractOperatorTest() throws IOException {
        try {
            initLdbcGraph();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        optimizeConfig = new OptimizeConfig();
        compilerConfig = new CompilerConfig();
        logicalPlanOptimizer = new LogicalPlanOptimizer(optimizeConfig, false, schema, 0, true);

        g = ldbcGraph.traversal();
    }

    private void initLdbcGraph() throws MetaException, IOException {
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

    private String getDirectory() {
        return "result/operator";
    }

    protected String getWriteDirectory() {
        throw new RuntimeException("Write you directory path here");
    }

    protected void assertResultPlan(String caseName, String resultContent) {
        try {
            String expectContent =
                    IOUtils.toString(
                            Thread.currentThread()
                                    .getContextClassLoader()
                                    .getResourceAsStream(getDirectory() + "/" + caseName),
                            "utf-8");
            Assert.assertEquals(StringUtils.trim(expectContent), StringUtils.trim(resultContent));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void executeTreeQuery(GraphTraversal traversal) {
        QueryFlowManager queryFlowManager = logicalPlanOptimizer.build(traversal);
        String resultContent = TextFormat.printToString(queryFlowManager.getQueryFlow().build());
        System.out.println(resultContent);
        if (writeFlag) {
            try {
                IOUtils.write(
                        resultContent,
                        new FileOutputStream(
                                new File(getWriteDirectory() + "/" + name.getMethodName())),
                        "utf-8");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            assertResultPlan(name.getMethodName(), resultContent);
        }
    }
}
