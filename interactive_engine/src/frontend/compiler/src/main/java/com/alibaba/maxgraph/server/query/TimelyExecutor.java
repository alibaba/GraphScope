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
package com.alibaba.maxgraph.server.query;

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.executor.ExecuteConfig;
import com.alibaba.maxgraph.compiler.executor.QueryExecutor;
import com.alibaba.maxgraph.compiler.optimizer.OperatorListManager;
import com.alibaba.maxgraph.compiler.optimizer.QueryFlowManager;
import com.alibaba.maxgraph.compiler.prepare.store.MemoryStatementStore;
import com.alibaba.maxgraph.compiler.prepare.store.PrepareStoreEntity;
import com.alibaba.maxgraph.compiler.prepare.store.StatementStore;
import com.alibaba.maxgraph.compiler.query.TimelyQuery;
import com.alibaba.maxgraph.rpc.TimelyResultProcessor;
import com.alibaba.maxgraph.sdkcommon.graph.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Executor for timely
 */
public class TimelyExecutor implements QueryExecutor<List<QueryResult>>, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TimelyExecutor.class);

    private final RpcConnector rpcConnector;
    private final StatementStore statementStore;
    private final boolean isAsyncGrpc;

    public TimelyExecutor(TimelyRpcConnector timelyRpcConnector) {
        this(timelyRpcConnector, new MemoryStatementStore(), true);
    }

    public TimelyExecutor(RpcConnector rpcConnector, StatementStore statementStore, boolean isAsyncGrpc) {
        this.rpcConnector = rpcConnector;
        this.statementStore = statementStore;
        this.isAsyncGrpc = isAsyncGrpc;
    }

    @Override
    public void execute(TimelyQuery timelyQuery, GraphSchema schema, long timeout, String queryId) {
        execute(timelyQuery, new ExecuteConfig(), schema, timeout, queryId);
    }

    @Override
    public void prepare(String prepareId, TimelyQuery timelyQuery, ExecuteConfig executeConfig ) {
        QueryFlowManager queryFlowManager = timelyQuery.getQueryFlowManager();
        checkArgument(queryFlowManager.checkValidPrepareFlow(), "There's no argument for prepare statement");

        try {
            if (statementStore.checkExist(prepareId)) {
                throw new RuntimeException("PREPARE " + prepareId + " fail for the same name statement has been exist.");
            }

            QueryFlowOuterClass.QueryFlow queryFlow = queryFlowManager.getQueryFlow().setQueryId(prepareId).build();
            rpcConnector.prepare(queryFlow, this.isAsyncGrpc);

            OperatorListManager operatorListManager = queryFlowManager.getOperatorListManager();
            statementStore.save(prepareId, new PrepareStoreEntity(operatorListManager.getPrepareEntityList(), operatorListManager.getLabelManager(), queryFlowManager.getResultValueType(), queryFlow));
            logger.info("PREPARE " + prepareId + " success");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(TimelyQuery timelyQuery, ExecuteConfig executeConfig, GraphSchema schema, long timeout, String queryId) {
        QueryFlowManager queryFlowManager = timelyQuery.getQueryFlowManager();
        try {
            rpcConnector.query(queryFlowManager.getQueryFlow().setQueryId(queryId), timelyQuery.getResultProcessor(), schema, timelyQuery.getGraph(), timeout, this.isAsyncGrpc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        rpcConnector.close();
    }

    public StatementStore getStatementStore() {
        return checkNotNull(statementStore);
    }

    @Override
    public void executePrepare(String prepareId, TimelyQuery timelyQuery, GraphSchema schema, String queryId) {
    //    timelyQuery.getQueryFlowManager().getQueryFlow();
        QueryFlowOuterClass.Query.Builder query = QueryFlowOuterClass.Query.newBuilder()
            .setDataflowId(prepareId)
            .setInput(timelyQuery.getQueryInput())
            .setQueryId(queryId);

        try {
            rpcConnector.executePrepare(query, timelyQuery.getResultProcessor(), schema, timelyQuery.getGraph(), this.isAsyncGrpc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void showProcessList(TimelyResultProcessor resultProcessor) {
        try {
            rpcConnector.showProcessList(resultProcessor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cancelDataflow(TimelyResultProcessor resultProcessor, String queryId) {
        try {
            rpcConnector.cancelDataflow(resultProcessor, queryId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
