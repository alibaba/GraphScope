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

import com.alibaba.maxgraph.api.query.QueryCallbackManager;
import com.alibaba.maxgraph.api.query.QueryStatus;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.common.rpc.RpcConfig;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.compiler.dfs.DfsTraversal;
import com.alibaba.maxgraph.compiler.executor.ExecuteConfig;
import com.alibaba.maxgraph.compiler.executor.QueryExecutor;
import com.alibaba.maxgraph.compiler.optimizer.LogicalPlanOptimizer;
import com.alibaba.maxgraph.compiler.optimizer.OptimizeConfig;
import com.alibaba.maxgraph.compiler.optimizer.QueryFlowManager;
import com.alibaba.maxgraph.compiler.prepare.store.StatementStore;
import com.alibaba.maxgraph.compiler.query.TimelyQuery;
import com.alibaba.maxgraph.logging.LogEvents.QueryEvent;
import com.alibaba.maxgraph.logging.LogEvents.QueryType;
import com.alibaba.maxgraph.logging.Logging;
import com.alibaba.maxgraph.sdkcommon.graph.DfsRequest;
import com.alibaba.maxgraph.server.query.*;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.alibaba.maxgraph.tinkerpop.strategies.MxGraphStepStrategy;
import com.alibaba.maxgraph.server.AbstractMixedOpProcessor;
import com.alibaba.maxgraph.server.AbstractMixedTraversalOpProcessor;
import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV3d0;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.alibaba.maxgraph.proto.RoleType.FRONTEND;

public class MixedTraversalOpProcessor extends AbstractMixedTraversalOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMixedOpProcessor.class);
    private final String graphName;
    private final int serverId;
    private SchemaFetcher schemaFetcher;
    private QueryExecutor timelyExecutor;
    private RemoteRpcConnector remoteRpcConnector;
    private QueryCallbackManager queryCallbackManager;
    private TinkerMaxGraph graph;
    private boolean globalPullGraphFlag;
    private boolean lambdaEnableFlag;

    public MixedTraversalOpProcessor(
            TinkerMaxGraph graph,
            InstanceConfig instanceConfig,
            RpcAddressFetcher executorAddressFetcher,
            SchemaFetcher schemaFetcher,
            StatementStore statementStore,
            QueryCallbackManager queryCallbackManager) {
        super(instanceConfig);
        this.graph = graph;
        this.schemaFetcher = schemaFetcher;
        this.globalPullGraphFlag = instanceConfig.getGlobalPullGraphFlag();
        this.lambdaEnableFlag = instanceConfig.getLambdaEnableFlag();
        RpcConnector rpcConnector =
                new PegasusRpcConnector(executorAddressFetcher, new RpcConfig());

        this.timelyExecutor =
                new TimelyExecutor(
                        rpcConnector, statementStore, instanceConfig.getAsyncGrpcQuery());
        this.remoteRpcConnector =
                new RemoteRpcConnector(
                        executorAddressFetcher, new RpcConfig(), false, RpcProcessorType.TRAVERSER);
        this.queryCallbackManager = queryCallbackManager;

        this.graphName = instanceConfig.getGraphName();
        int serverId = instanceConfig.getInt("node.idx", -1);
        if (serverId == -1) {
            serverId = instanceConfig.getServerId();
        }
        this.serverId = serverId;
    }

    @Override
    protected Object createDfsTraversal(
            GraphTraversal.Admin<?, ?> traversal, DfsRequest dfsRequest) {
        return new DfsTraversal(
                traversal,
                dfsRequest.getStart(),
                dfsRequest.getEnd(),
                dfsRequest.getBatchSize(),
                dfsRequest.isOrder());
    }

    @Override
    protected void processTraversal(Context context, Object object, Graph graph, long timeout) {
        String queryId = String.valueOf(ThreadLocalRandom.current().nextLong());
        Stopwatch timer = Stopwatch.createStarted();
        try {
            Logging.query(
                    this.graphName,
                    FRONTEND,
                    this.serverId,
                    queryId,
                    QueryType.EXECUTE,
                    QueryEvent.FRONT_RECEIVED,
                    null,
                    null,
                    null,
                    "");
            Long resultNum = doProcessTraversal(context, object, graph, timeout, queryId, timer);
            Logging.query(
                    this.graphName,
                    FRONTEND,
                    this.serverId,
                    queryId,
                    QueryType.EXECUTE,
                    QueryEvent.FRONT_FINISH,
                    timer.elapsed(TimeUnit.NANOSECONDS),
                    resultNum,
                    true,
                    "");
        } catch (Exception e) {
            Logging.query(
                    this.graphName,
                    FRONTEND,
                    this.serverId,
                    queryId,
                    QueryType.EXECUTE,
                    QueryEvent.FRONT_FINISH,
                    timer.elapsed(TimeUnit.NANOSECONDS),
                    null,
                    false,
                    "");
            throw e;
        }
    }

    private Long doProcessTraversal(
            Context context,
            Object object,
            Graph graph,
            long timeout,
            String queryId,
            Stopwatch timer) {
        Pair<GraphSchema, Long> snapshotSchema;
        GraphSchema schema;

        Long resultNum = 0L;
        if (object instanceof GraphTraversal.Admin || object instanceof DfsTraversal) {
            QueryStatus queryStatus;
            GraphTraversal.Admin traversal =
                    (object instanceof GraphTraversal.Admin)
                            ? GraphTraversal.Admin.class.cast(object)
                            : (DfsTraversal.class.cast(object)).getTraversal();
            String queryString = traversal.toString();
            logger.info("Receive traversal query=>" + queryString);

            if (!traversal.isLocked()) {
                traversal
                        .getStrategies()
                        .removeStrategies(
                                ProfileStrategy.class,
                                MxGraphStepStrategy.class,
                                FilterRankingStrategy.class);
            }
            traversal.applyStrategies();
            NettyVertexRpcProcessor nettyVertexRpcProcessor;
            QueryFlowManager queryFlowManager;

            // 保证一查看到snapshotId就开始维护query_status
            synchronized (queryCallbackManager) {
                snapshotSchema = schemaFetcher.getSchemaSnapshotPair();
                queryStatus = queryCallbackManager.beforeExecution(snapshotSchema.getRight());
            }
            schema = snapshotSchema.getLeft();
            LogicalPlanOptimizer planOptimizer =
                    new LogicalPlanOptimizer(
                            new OptimizeConfig(),
                            this.globalPullGraphFlag,
                            schema,
                            snapshotSchema.getRight(),
                            this.lambdaEnableFlag);
            final int resultIterationBatchSize =
                    (Integer)
                            context.getRequestMessage()
                                    .optionalArgs(Tokens.ARGS_BATCH_SIZE)
                                    .orElse(this.resultIterationBatchSize);
            nettyVertexRpcProcessor =
                    new NettyTraverserVertexProcessor(context, resultIterationBatchSize, false);
            try {
                queryFlowManager =
                        (object instanceof GraphTraversal.Admin)
                                ? planOptimizer.build(GraphTraversal.class.cast(traversal))
                                : planOptimizer.build(DfsTraversal.class.cast(object));
            } catch (IllegalArgumentException iae) {
                if (iae.getMessage().contains("MaxGraphIoStep")) {
                    logger.info("do maxgraph io step");
                    while (traversal.hasNext()) {
                        logger.info("maxgraph io hasNext");
                    }
                    nettyVertexRpcProcessor.finish(ResponseStatusCode.SUCCESS);
                    return 0L;
                }
                throw iae;
            }

            try {
                boolean isLambdaExisted =
                        TraversalHelper.anyStepRecursively(
                                s -> s instanceof LambdaHolder, (Traversal.Admin<?, ?>) traversal);
                queryFlowManager.getQueryFlow().setFrontId(serverId);
                if (this.lambdaEnableFlag && isLambdaExisted) {
                    final ObjectMapper mapper =
                            GraphSONMapper.build()
                                    .version(GraphSONVersion.V3_0)
                                    .addCustomModule(GraphSONXModuleV3d0.build().create(false))
                                    .create()
                                    .createMapper();
                    Bytecode bytecode =
                            (Bytecode)
                                    context.getRequestMessage().getArgs().get(Tokens.ARGS_GREMLIN);
                    byte[] bytecodeByte = mapper.writeValueAsBytes(bytecode);
                    queryFlowManager
                            .getQueryFlow()
                            .setLambdaExisted(isLambdaExisted)
                            .setBytecode(ByteString.copyFrom(bytecodeByte));
                }

                GremlinResultTransform gremlinResultTransform =
                        new GremlinResultTransform(
                                remoteRpcConnector,
                                nettyVertexRpcProcessor,
                                this.graph,
                                queryFlowManager.getResultValueType(),
                                vertexCacheFlag);
                NettyResultProcessor nettyResultProcessor =
                        new NettyResultProcessor(
                                queryId,
                                traversal.toString(),
                                context,
                                new ExecuteConfig().getBatchQuerySize(),
                                resultIterationBatchSize,
                                false);
                nettyResultProcessor.setSchema(schema);
                nettyResultProcessor.setResultTransform(gremlinResultTransform);
                nettyResultProcessor.setLabelIndexNameList(
                        queryFlowManager.getTreeNodeLabelManager().getUserIndexLabelList());
                TimelyQuery timelyQuery =
                        new TimelyQuery(queryFlowManager, nettyResultProcessor, this.graph);
                Logging.query(
                        this.graphName,
                        FRONTEND,
                        this.serverId,
                        queryId,
                        QueryType.EXECUTE,
                        QueryEvent.PLAN_GENERATED,
                        timer.elapsed(TimeUnit.NANOSECONDS),
                        null,
                        null,
                        "");
                timelyExecutor.execute(timelyQuery, schema, timeout, queryId);
                resultNum = nettyResultProcessor.total();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            } finally {
                queryCallbackManager.afterExecution(queryStatus);
            }
        } else {
            throw new IllegalArgumentException("Not support to process=>" + object);
        }
        return resultNum;
    }
}
