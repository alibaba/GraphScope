package com.alibaba.maxgraph.tests.frontend.compiler;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.v2.frontend.compiler.cost.statistics.CostDataStatistics;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.CompilerConfig;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.LogicalPlanOptimizer;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.OptimizeConfig;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.QueryFlowManager;
import com.alibaba.maxgraph.v2.frontend.graph.MaxGraphTraversalSource;
import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import com.alibaba.maxgraph.v2.frontend.graph.memory.DefaultMaxGraphReader;
import com.alibaba.maxgraph.v2.frontend.graph.memory.DefaultMaxGraphWriter;
import com.alibaba.maxgraph.v2.frontend.graph.memory.DefaultMemoryGraph;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultGraphSchema;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultSchemaFetcher;
import com.alibaba.maxgraph.v2.common.schema.GraphSchemaMapper;
import com.google.protobuf.TextFormat;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AbstractOperatorTest {
    @Rule
    public TestName name = new TestName();

    protected GraphSchema schema;
    private SnapshotMaxGraph ldbcGraph;

    protected MaxGraphTraversalSource g;

    protected OptimizeConfig optimizeConfig;
    protected CompilerConfig compilerConfig;

    protected LogicalPlanOptimizer logicalPlanOptimizer;

    protected boolean writeFlag = Boolean.valueOf(
            IOUtils.toString(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream("result/writeFlag"), "utf-8"));

    protected AbstractOperatorTest() throws IOException {
        try {
            initLdbcGraph();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        optimizeConfig = new OptimizeConfig();
        compilerConfig = new CompilerConfig();
        logicalPlanOptimizer = new LogicalPlanOptimizer(schema, 0);

        g = (MaxGraphTraversalSource) ldbcGraph.traversal();
    }

    private void initLdbcGraph() throws IOException {
        String schemaValue = IOUtils.toString(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("ldbc.schema"),
                "utf-8");
        schema = GraphSchemaMapper.parseFromJson(schemaValue).toGraphSchema();
        CostDataStatistics.initialize(new DefaultSchemaFetcher(schema));

        DefaultMemoryGraph memoryGraph = DefaultMemoryGraph.getGraph();
        DefaultMaxGraphWriter writer = new DefaultMaxGraphWriter((DefaultGraphSchema) schema, memoryGraph);
        SchemaFetcher schemaFetcher = new DefaultSchemaFetcher(this.schema);
        ldbcGraph = new SnapshotMaxGraph();
        ldbcGraph.initialize(new DefaultMaxGraphReader(writer, memoryGraph, schemaFetcher),
                new DefaultMaxGraphWriter((DefaultGraphSchema) this.schema, memoryGraph),
                schemaFetcher);
    }

    private String getDirectory() {
        return "result/operator";
    }

    protected String getWriteDirectory() {
        return "/Users/zjureel/Code/graphscope/src/v2/src/test/resources/result/operator";
    }

    protected void assertResultPlan(String caseName, String resultContent) {
        try {
            String expectContent = IOUtils.toString(Thread.currentThread().getContextClassLoader().getResourceAsStream(getDirectory() + "/" + caseName), "utf-8");
            assertEquals(StringUtils.trim(expectContent), StringUtils.trim(resultContent));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void executeTreeQuery(GraphTraversal traversal) {
        QueryFlowManager queryFlowManager = logicalPlanOptimizer.build(traversal);
        String resultContent = TextFormat.printer().printToString(queryFlowManager.getQueryFlow().build());
        System.out.println(resultContent);
        if (writeFlag) {
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
