/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.gremlin.plugin.processor;

import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.gremlin.plugin.QueryStatusCallback;
import com.alibaba.graphscope.gremlin.resultx.GremlinRecordParser;
import com.alibaba.graphscope.gremlin.resultx.GremlinResultProcessor;
import com.alibaba.graphscope.gremlin.resultx.ResultSchema;
import com.google.common.base.Preconditions;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.Context;

import java.util.function.Supplier;

public class LifeCycleSupplier implements Supplier<GremlinExecutor.LifeCycle> {
    private final GraphPlanner planner;
    private final ExecutionClient client;
    private final Context ctx;
    private final long queryId;
    private final String queryName;
    private final IrMeta meta;
    private final QueryStatusCallback statusCallback;

    public LifeCycleSupplier(
            Context ctx,
            GraphPlanner planner,
            ExecutionClient client,
            long queryId,
            String queryName,
            IrMeta meta,
            QueryStatusCallback statusCallback) {
        this.ctx = ctx;
        this.planner = planner;
        this.client = client;
        this.queryId = queryId;
        this.queryName = queryName;
        this.meta = meta;
        this.statusCallback = statusCallback;
    }

    @Override
    public GremlinExecutor.LifeCycle get() {
        QueryTimeoutConfig timeoutConfig = new QueryTimeoutConfig(ctx.getRequestTimeout());
        return GremlinExecutor.LifeCycle.build()
                .evaluationTimeoutOverride(timeoutConfig.getExecutionTimeoutMS())
                .beforeEval(
                        b -> {
                            b.put("graph.planner", planner);
                            b.put("graph.meta", meta);
                        })
                .withResult(
                        o -> {
                            try {
                                Preconditions.checkArgument(
                                        o instanceof GraphPlanner.Summary,
                                        "input of 'withResult' is invalid, expect type=%s, actual"
                                                + " type=%s",
                                        GraphPlanner.Summary.class,
                                        o.getClass());
                                GraphPlanner.Summary summary = (GraphPlanner.Summary) o;
                                statusCallback
                                        .getQueryLogger()
                                        .info("ir plan {}", summary.getPhysicalPlan().explain());
                                ResultSchema resultSchema =
                                        new ResultSchema(summary.getLogicalPlan());
                                this.client.submit(
                                        new ExecutionRequest(
                                                queryId,
                                                queryName,
                                                summary.getLogicalPlan(),
                                                summary.getPhysicalPlan()),
                                        new GremlinResultProcessor(
                                                ctx,
                                                statusCallback,
                                                new GremlinRecordParser(resultSchema),
                                                resultSchema),
                                        timeoutConfig);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
                .create();
    }
}
