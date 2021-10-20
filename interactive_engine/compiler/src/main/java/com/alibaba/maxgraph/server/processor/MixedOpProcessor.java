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
package com.alibaba.maxgraph.server.processor;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.api.query.QueryCallbackManager;
import com.alibaba.maxgraph.api.query.QueryStatus;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.common.rpc.RpcConfig;
import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.compiler.cost.CostModelManager;
import com.alibaba.maxgraph.compiler.cost.CostPath;
import com.alibaba.maxgraph.compiler.cost.statistics.CostDataStatistics;
import com.alibaba.maxgraph.compiler.dfs.DfsTraversal;
import com.alibaba.maxgraph.compiler.executor.ExecuteConfig;
import com.alibaba.maxgraph.compiler.optimizer.LogicalPlanOptimizer;
import com.alibaba.maxgraph.compiler.optimizer.OptimizeConfig;
import com.alibaba.maxgraph.compiler.optimizer.QueryFlowManager;
import com.alibaba.maxgraph.compiler.prepare.PreparedExecuteParam;
import com.alibaba.maxgraph.compiler.prepare.PreparedTraversal;
import com.alibaba.maxgraph.compiler.prepare.store.StatementStore;
import com.alibaba.maxgraph.compiler.query.TimelyQuery;
import com.alibaba.maxgraph.compiler.tree.TreeBuilder;
import com.alibaba.maxgraph.compiler.tree.TreeManager;
import com.alibaba.maxgraph.compiler.tree.value.ListValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.logging.LogEvents.QueryEvent;
import com.alibaba.maxgraph.logging.LogEvents.QueryType;
import com.alibaba.maxgraph.logging.Logging;
import com.alibaba.maxgraph.rpc.TimelyResultProcessor;
import com.alibaba.maxgraph.sdkcommon.graph.CancelDataflow;
import com.alibaba.maxgraph.sdkcommon.graph.EstimateRequest;
import com.alibaba.maxgraph.sdkcommon.graph.ShowPlanPathListRequest;
import com.alibaba.maxgraph.sdkcommon.graph.ShowProcessListQuery;
import com.alibaba.maxgraph.api.manager.RecordProcessorManager;
import com.alibaba.maxgraph.sdkcommon.graph.StatisticsRequest;
import com.alibaba.maxgraph.server.query.*;
import com.alibaba.maxgraph.compiler.exception.RetryGremlinException;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.alibaba.maxgraph.structure.manager.record.AddEdgeManager;
import com.alibaba.maxgraph.structure.manager.record.AddVertexManager;
import com.alibaba.maxgraph.structure.manager.record.DelEdgeManager;
import com.alibaba.maxgraph.structure.manager.record.DelVertexManager;
import com.alibaba.maxgraph.structure.manager.record.RecordManager;
import com.alibaba.maxgraph.structure.manager.record.UpdateEdgeManager;
import com.alibaba.maxgraph.structure.manager.record.UpdateVertexManager;
import com.alibaba.maxgraph.server.AbstractMixedOpProcessor;
import com.alibaba.maxgraph.tinkerpop.traversal.MaxGraphTraversalSource;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Mixed processor would try to query from timely service, if there's any exception, it will query
 * from tinkerpop
 */
public class MixedOpProcessor extends AbstractMixedOpProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(MixedOpProcessor.class);

    private final String graphName;
    private final int serverId;
    private SchemaFetcher schemaFetcher;
    private TimelyExecutor timelyExecutor;
    private RemoteRpcConnector remoteRpcConnector;
    private RemoteRpcConnector httpRpcConnector;
    private RecordProcessorManager recordProcessorManager;
    private QueryCallbackManager queryCallbackManager;
    private TinkerMaxGraph graph;

    private ExecuteConfig executeConfig = new ExecuteConfig();
    private boolean lambdaEnableFlag;

    public MixedOpProcessor(
            TinkerMaxGraph graph,
            InstanceConfig instanceConfig,
            RpcAddressFetcher executorAddressFetcher,
            SchemaFetcher schemaFetcher,
            StatementStore statementStore,
            RecordProcessorManager recordProcessorManager,
            QueryCallbackManager queryCallbackManager) {
        super(instanceConfig);

        this.graph = graph;
        this.schemaFetcher = schemaFetcher;

        RpcConnector rpcConnector =
                new PegasusRpcConnector(executorAddressFetcher, new RpcConfig());

        this.timelyExecutor =
                new TimelyExecutor(
                        rpcConnector, statementStore, instanceConfig.getAsyncGrpcQuery());
        this.lambdaEnableFlag = instanceConfig.getLambdaEnableFlag();
        this.remoteRpcConnector =
                new RemoteRpcConnector(
                        executorAddressFetcher, new RpcConfig(), false, RpcProcessorType.NETTY);
        this.httpRpcConnector =
                new RemoteRpcConnector(
                        executorAddressFetcher, new RpcConfig(), false, RpcProcessorType.MEMORY);
        this.recordProcessorManager = recordProcessorManager;
        this.queryCallbackManager = queryCallbackManager;

        this.graphName = instanceConfig.getGraphName();
        this.serverId = instanceConfig.getInt("node.idx", -1);
    }

    @Override
    protected void processGraphTraversal(
            String script, Context context, Object traversal, long timeout) {
        String queryId = String.valueOf(ThreadLocalRandom.current().nextLong());
        Stopwatch timer = Stopwatch.createStarted();
        QueryType queryType = QueryType.EXECUTE;
        if (traversal instanceof PreparedTraversal) {
            queryType = QueryType.PREPARE;
        } else if (traversal instanceof PreparedExecuteParam) {
            queryType = QueryType.QUERY;
        }
        Logging.query(
                this.graphName,
                com.alibaba.maxgraph.proto.RoleType.FRONTEND,
                this.serverId,
                queryId,
                queryType,
                QueryEvent.FRONT_RECEIVED,
                null,
                null,
                null,
                script);

        try {
            Long totalResultNum =
                    doProcessGraphTraversal(script, context, traversal, timeout, queryId, timer);
            Logging.query(
                    this.graphName,
                    com.alibaba.maxgraph.proto.RoleType.FRONTEND,
                    this.serverId,
                    queryId,
                    QueryType.EXECUTE,
                    QueryEvent.FRONT_FINISH,
                    timer.elapsed(TimeUnit.NANOSECONDS),
                    totalResultNum,
                    true,
                    script);
        } catch (Exception e) {
            Logging.query(
                    this.graphName,
                    com.alibaba.maxgraph.proto.RoleType.FRONTEND,
                    this.serverId,
                    queryId,
                    QueryType.EXECUTE,
                    QueryEvent.FRONT_FINISH,
                    timer.elapsed(TimeUnit.NANOSECONDS),
                    null,
                    false,
                    script);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Object> processHttpGraphTraversal(
            String script, Object traversal, long timeout, FullHttpRequest request)
            throws Exception {
        if (traversal instanceof GraphTraversal || traversal instanceof DfsTraversal) {
            String queryId = String.valueOf(ThreadLocalRandom.current().nextLong());
            Stopwatch timer = Stopwatch.createStarted();
            RemoteRpcProcessor remoteRpcProcessor = new DefaultVertexRpcProcessor();
            MemoryResultProcessor resultProcessor =
                    new MemoryResultProcessor(
                            executeConfig.getBatchQuerySize(), resultIterationBatchSize, queryId);
            processQueryTraversal(
                    script,
                    traversal,
                    timeout,
                    queryId,
                    timer,
                    this.httpRpcConnector,
                    remoteRpcProcessor,
                    resultProcessor);

            return resultProcessor.getResultList();
        } else {
            throw new IllegalArgumentException("Not support " + script + " in http request");
        }
    }

    private Long doProcessGraphTraversal(
            String script,
            Context context,
            Object traversal,
            long timeout,
            String queryId,
            Stopwatch timer)
            throws Exception {
        Long totalResultNum = 0L;
        Pair<GraphSchema, Long> snapshotSchema;
        GraphSchema schema;

        if (traversal instanceof GraphTraversal || traversal instanceof DfsTraversal) {
            final int resultIterationBatchSize =
                    (Integer)
                            context.getRequestMessage()
                                    .optionalArgs(Tokens.ARGS_BATCH_SIZE)
                                    .orElse(this.resultIterationBatchSize);
            NettyVertexRpcProcessor nettyVertexRpcProcessor =
                    new NettyVertexRpcProcessor(context, resultIterationBatchSize, false);
            AbstractResultProcessor nettyResultProcessor =
                    new NettyResultProcessor(
                            queryId,
                            script,
                            context,
                            executeConfig.getBatchQuerySize(),
                            resultIterationBatchSize,
                            false);

            totalResultNum =
                    processQueryTraversal(
                            script,
                            traversal,
                            timeout,
                            queryId,
                            timer,
                            this.remoteRpcConnector,
                            nettyVertexRpcProcessor,
                            nettyResultProcessor);
        } else {
            snapshotSchema = this.schemaFetcher.getSchemaSnapshotPair();
            schema = snapshotSchema.getLeft();
            if (traversal instanceof PreparedTraversal) {
                throw new UnsupportedOperationException();
            } else if (traversal instanceof PreparedExecuteParam) {
                throw new UnsupportedOperationException();
            } else if (traversal instanceof ShowProcessListQuery) {
                TimelyResultProcessor nettyResultProcessor =
                        newNettyResultProcessor(queryId, script, context, graph, schema);
                timelyExecutor.showProcessList(nettyResultProcessor);
            } else if (traversal instanceof CancelDataflow) {
                NettyResultProcessor nettyResultProcessor =
                        newNettyResultProcessor(queryId, script, context, graph, schema);
                timelyExecutor.cancelDataflow(
                        nettyResultProcessor, ((CancelDataflow) traversal).queryId);
            } else if (traversal instanceof RecordManager) {
                Object result = processRecordManager(RecordManager.class.cast(traversal));
                writeResultList(context, Lists.newArrayList(result), ResponseStatusCode.SUCCESS);
            } else if (traversal instanceof EstimateRequest) {
                writeResultList(
                        context,
                        Lists.newArrayList(
                                processEstimateManager((EstimateRequest) traversal, timeout)),
                        ResponseStatusCode.SUCCESS);
            } else if (traversal instanceof StatisticsRequest) {
                CostDataStatistics costDataStatistics = CostDataStatistics.getInstance();
                writeResultList(
                        context,
                        Lists.newArrayList(costDataStatistics.formatJson()),
                        ResponseStatusCode.SUCCESS);
            } else if (traversal instanceof ShowPlanPathListRequest) {
                ShowPlanPathListRequest showPlanPathListRequest =
                        (ShowPlanPathListRequest) traversal;
                writeResultList(
                        context,
                        Lists.newArrayList(
                                buildCostPathList(showPlanPathListRequest.getTraversal())),
                        ResponseStatusCode.SUCCESS);
            } else if (traversal instanceof Element) {
                writeResultList(
                        context,
                        Lists.newArrayList((Element) traversal),
                        ResponseStatusCode.SUCCESS);
            } else if (traversal instanceof List) {
                writeResultList(context, (List) traversal, ResponseStatusCode.SUCCESS);
            } else if (traversal instanceof GraphSchema) {
                writeResultList(
                        context,
                        Lists.newArrayList(((GraphSchema) traversal).formatJson()),
                        ResponseStatusCode.SUCCESS);
            } else if (traversal instanceof String) {
                writeResultList(context, Lists.newArrayList(traversal), ResponseStatusCode.SUCCESS);
            } else if (traversal != null
                    && (!(traversal instanceof String)
                            || !StringUtils.isEmpty(traversal.toString()))) {
                throw new IllegalArgumentException(traversal.toString());
            }
        }
        return totalResultNum;
    }

    private List<String> buildCostPathList(GraphTraversal traversal) {
        TreeBuilder treeBuilder =
                TreeBuilder.newTreeBuilder(
                        this.schemaFetcher.getSchemaSnapshotPair().getLeft(),
                        new OptimizeConfig(),
                        true);
        TreeManager treeManager = treeBuilder.build(traversal);
        treeManager.optimizeTree();
        CostModelManager costModelManager = treeManager.optimizeCostModel();
        List<CostPath> pathList = costModelManager.getPathList();
        if (pathList == null || pathList.isEmpty()) {
            return Lists.newArrayList();
        }
        return pathList.stream().map(CostPath::toString).collect(Collectors.toList());
    }

    private Object processEstimateManager(EstimateRequest request, long timeout)
            throws RetryGremlinException {
        Stopwatch timer = Stopwatch.createStarted();

        CostDataStatistics statistics = CostDataStatistics.getInstance();
        TinkerMaxGraph emptyGraph = new TinkerMaxGraph(null, null, null);
        MaxGraphTraversalSource g = (MaxGraphTraversalSource) emptyGraph.traversal();

        GraphSchema graphSchema = schemaFetcher.getSchemaSnapshotPair().getLeft();
        Map<String, Double> vertexCountList = Maps.newHashMap();
        for (GraphElement vertex : graphSchema.getVertexList()) {
            String queryId = String.valueOf(ThreadLocalRandom.current().nextLong());
            GraphTraversal vertexQuery = g.estimateVCount(vertex.getLabel());
            RemoteRpcProcessor remoteRpcProcessor = new DefaultVertexRpcProcessor();
            MemoryResultProcessor resultProcessor =
                    new MemoryResultProcessor(
                            executeConfig.getBatchQuerySize(), resultIterationBatchSize, queryId);
            processQueryTraversal(
                    vertexQuery.toString(),
                    vertexQuery,
                    timeout,
                    queryId,
                    timer,
                    this.httpRpcConnector,
                    remoteRpcProcessor,
                    resultProcessor);
            double countValue =
                    Double.parseDouble(resultProcessor.getResultList().get(0).toString());
            vertexCountList.put(vertex.getLabel(), countValue);
        }

        Map<String, Double> edgeCountList = Maps.newHashMap();
        for (GraphElement edge : graphSchema.getEdgeList()) {
            GraphTraversal edgeQuery = g.estimateECount(edge.getLabel());
            String queryId = String.valueOf(ThreadLocalRandom.current().nextLong());
            RemoteRpcProcessor remoteRpcProcessor = new DefaultVertexRpcProcessor();
            MemoryResultProcessor resultProcessor =
                    new MemoryResultProcessor(
                            executeConfig.getBatchQuerySize(), resultIterationBatchSize, queryId);
            processQueryTraversal(
                    edgeQuery.toString(),
                    edgeQuery,
                    timeout,
                    queryId,
                    timer,
                    this.httpRpcConnector,
                    remoteRpcProcessor,
                    resultProcessor);
            double countValue =
                    Double.parseDouble(resultProcessor.getResultList().get(0).toString());
            edgeCountList.put(edge.getLabel(), countValue);
        }

        for (Map.Entry<String, Double> entry : vertexCountList.entrySet()) {
            statistics.addVertexCount(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, Double> entry : edgeCountList.entrySet()) {
            statistics.addEdgeCount(entry.getKey(), entry.getValue());
        }

        return "Estimate vertex/edge count success";
    }

    private Long processQueryTraversal(
            String script,
            Object traversal,
            long timeout,
            String queryId,
            Stopwatch timer,
            RemoteRpcConnector remoteRpcConnector,
            RemoteRpcProcessor remoteRpcProcessor,
            AbstractResultProcessor nettyResultProcessor)
            throws RetryGremlinException {
        Pair<GraphSchema, Long> snapshotSchema;
        GraphSchema schema;
        Long totalResultNum;
        QueryStatus queryStatus;
        QueryFlowManager queryFlowManager;

        // 保证一查看到snapshotId就开始维护query_status
        synchronized (queryCallbackManager) {
            snapshotSchema = this.schemaFetcher.getSchemaSnapshotPair();
            queryStatus = queryCallbackManager.beforeExecution(snapshotSchema.getRight());
        }
        schema = snapshotSchema.getLeft();

        LogicalPlanOptimizer logicalPlanOptimizer =
                new LogicalPlanOptimizer(
                        new OptimizeConfig(),
                        this.globalPullGraphFlag,
                        schema,
                        snapshotSchema.getRight(),
                        this.lambdaEnableFlag);
        queryFlowManager =
                (traversal instanceof GraphTraversal)
                        ? logicalPlanOptimizer.build(GraphTraversal.class.cast(traversal))
                        : logicalPlanOptimizer.build(DfsTraversal.class.cast(traversal));
        boolean isLambdaExisted =
                TraversalHelper.anyStepRecursively(
                        s -> s instanceof LambdaHolder, (Traversal.Admin<?, ?>) traversal);
        queryFlowManager.getQueryFlow().setScript(script).setFrontId(serverId);
        if (this.lambdaEnableFlag && isLambdaExisted) {
            queryFlowManager.getQueryFlow().setLambdaExisted(isLambdaExisted);
        }
        nettyResultProcessor.setResultTransform(
                new GremlinResultTransform(
                        remoteRpcConnector,
                        remoteRpcProcessor,
                        this.graph,
                        queryFlowManager.getResultValueType(),
                        vertexCacheFlag));
        nettyResultProcessor.setLabelIndexNameList(
                queryFlowManager.getTreeNodeLabelManager().getUserIndexLabelList());
        nettyResultProcessor.setSchema(schema);

        try {
            TimelyQuery timelyQuery =
                    new TimelyQuery(queryFlowManager, nettyResultProcessor, this.graph);
            Logging.query(
                    this.graphName,
                    com.alibaba.maxgraph.proto.RoleType.FRONTEND,
                    this.serverId,
                    queryId,
                    QueryType.EXECUTE,
                    QueryEvent.PLAN_GENERATED,
                    timer.elapsed(TimeUnit.NANOSECONDS),
                    null,
                    null,
                    script);
            timelyExecutor.execute(timelyQuery, schema, timeout, queryId);
            totalResultNum = nettyResultProcessor.total();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            queryCallbackManager.afterExecution(queryStatus);
        }
        return totalResultNum;
    }

    private Object processRecordManager(RecordManager recordManager) {
        if (recordManager instanceof AddVertexManager) {
            AddVertexManager addVertexManager = AddVertexManager.class.cast(recordManager);
            return recordProcessorManager.addVertex(addVertexManager);
        } else if (recordManager instanceof AddEdgeManager) {
            AddEdgeManager addEdgeManager = AddEdgeManager.class.cast(recordManager);
            return recordProcessorManager.addEdge(addEdgeManager);
        } else if (recordManager instanceof DelVertexManager) {
            DelVertexManager delVertexManager = DelVertexManager.class.cast(recordManager);
            recordProcessorManager.deleteVertex(delVertexManager);
            return "Delete vertex[" + delVertexManager.getVertexId().toString() + "] succ";
        } else if (recordManager instanceof DelEdgeManager) {
            DelEdgeManager delEdgeManager = DelEdgeManager.class.cast(recordManager);
            recordProcessorManager.deleteEdge(delEdgeManager);
            return "Delete edge with id " + delEdgeManager.getEdgeId() + " succ";
        } else if (recordManager instanceof UpdateVertexManager) {
            UpdateVertexManager updateVertexManager = UpdateVertexManager.class.cast(recordManager);
            return recordProcessorManager.updateVertex(updateVertexManager);
        } else if (recordManager instanceof UpdateEdgeManager) {
            UpdateEdgeManager updateEdgeManager = UpdateEdgeManager.class.cast(recordManager);
            recordProcessorManager.updateEdge(updateEdgeManager);
            return "Update edge["
                    + updateEdgeManager.getLabel()
                    + "-"
                    + updateEdgeManager.getEdgeId()
                    + "] succ";
        } else {
            throw new UnsupportedOperationException(recordManager.toString());
        }
    }

    private NettyResultProcessor newNettyResultProcessor(
            String queryId, String script, Context context, Graph graph, GraphSchema schema) {
        NettyVertexRpcProcessor nettyVertexRpcProcessor =
                new NettyVertexRpcProcessor(context, resultIterationBatchSize, false);
        GremlinResultTransform gremlinResultTransform =
                new GremlinResultTransform(
                        remoteRpcConnector,
                        nettyVertexRpcProcessor,
                        this.graph,
                        new ListValueType(new ValueValueType(Message.VariantType.VT_STRING)),
                        vertexCacheFlag);
        NettyResultProcessor nettyResultProcessor =
                new NettyResultProcessor(
                        queryId,
                        script,
                        context,
                        executeConfig.getBatchQuerySize(),
                        resultIterationBatchSize,
                        false);
        nettyResultProcessor.setSchema(schema);
        nettyResultProcessor.setResultTransform(gremlinResultTransform);
        return nettyResultProcessor;
    }
}
