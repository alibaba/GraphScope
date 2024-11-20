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
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.QueryCache;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gremlin.plugin.QueryStatusCallback;
import com.alibaba.graphscope.gremlin.resultx.GremlinRecordParser;
import com.alibaba.graphscope.gremlin.resultx.GremlinResultProcessor;
import com.alibaba.graphscope.gremlin.resultx.ResultSchema;
import com.google.common.base.Preconditions;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.Context;

import java.math.BigInteger;
import java.util.List;
import java.util.function.Supplier;

public class LifeCycleSupplier implements Supplier<GremlinExecutor.LifeCycle> {
    private final Configs configs;
    private final QueryCache queryCache;
    private final GraphPlanner graphPlanner;
    private final ExecutionClient client;
    private final Context ctx;
    private final BigInteger queryId;
    private final String queryName;
    private final IrMeta meta;
    private final QueryStatusCallback statusCallback;
    private final QueryTimeoutConfig timeoutConfig;

    public LifeCycleSupplier(
            Configs configs,
            Context ctx,
            QueryCache queryCache,
            GraphPlanner graphPlanner,
            ExecutionClient client,
            BigInteger queryId,
            String queryName,
            IrMeta meta,
            QueryStatusCallback statusCallback,
            QueryTimeoutConfig timeoutConfig) {
        this.configs = configs;
        this.ctx = ctx;
        this.queryCache = queryCache;
        this.graphPlanner = graphPlanner;
        this.client = client;
        this.queryId = queryId;
        this.queryName = queryName;
        this.meta = meta;
        this.statusCallback = statusCallback;
        this.timeoutConfig = timeoutConfig;
    }

    @Override
    public GremlinExecutor.LifeCycle get() {
        return GremlinExecutor.LifeCycle.build()
                .evaluationTimeoutOverride(timeoutConfig.getExecutionTimeoutMS())
                .beforeEval(
                        b -> {
                            b.put("graph.query.cache", queryCache);
                            b.put("graph.planner", graphPlanner);
                            b.put("graph.meta", meta);
                            b.put("graph.query.logger", statusCallback.getQueryLogger());
                        })
                .withResult(
                        o -> {
                            try {
                                Preconditions.checkArgument(
                                        o instanceof QueryCache.Value,
                                        "input of 'withResult' is invalid, expect type=%s, actual"
                                                + " type=%s",
                                        QueryCache.Value.class,
                                        o.getClass());
                                QueryCache.Value value = (QueryCache.Value) o;
                                GraphPlanner.Summary summary = value.summary;
                                statusCallback
                                        .getQueryLogger()
                                        .info(
                                                "logical IR plan \n\n {} \n\n",
                                                summary.getLogicalPlan().explain());
                                statusCallback
                                        .getQueryLogger()
                                        .debug(
                                                "physical IR plan {}",
                                                summary.getPhysicalPlan().explain());
                                ResultSchema resultSchema =
                                        new ResultSchema(summary.getLogicalPlan());
                                GremlinResultProcessor listener =
                                        new GremlinResultProcessor(
                                                configs,
                                                ctx,
                                                new GremlinRecordParser(resultSchema),
                                                resultSchema,
                                                statusCallback,
                                                timeoutConfig);
                                if (value.result != null && value.result.isCompleted) {
                                    List<IrResult.Results> records = value.result.records;
                                    records.forEach(k -> listener.onNext(k.getRecord()));
                                    listener.onCompleted();
                                } else {
                                    this.client.submit(
                                            new ExecutionRequest(
                                                    queryId,
                                                    queryName,
                                                    summary.getLogicalPlan(),
                                                    summary.getPhysicalPlan()),
                                            listener,
                                            timeoutConfig,
                                            statusCallback.getQueryLogger());
                                    statusCallback
                                            .getQueryLogger()
                                            .info("[query][submitted]: physical IR submitted");
                                }
                                // request results from remote engine in a blocking way
                                listener.request();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
                .create();
    }
}
