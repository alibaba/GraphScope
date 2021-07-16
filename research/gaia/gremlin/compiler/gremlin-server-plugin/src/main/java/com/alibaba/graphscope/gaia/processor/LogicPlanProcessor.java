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
package com.alibaba.graphscope.gaia.processor;

import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.idmaker.TagIdMaker;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.graphscope.gaia.store.GraphType;
import com.alibaba.pegasus.builder.AbstractBuilder;
import com.alibaba.graphscope.gaia.plan.translator.TraversalTranslator;
import com.alibaba.graphscope.gaia.plan.translator.builder.PlanConfig;
import com.alibaba.graphscope.gaia.plan.translator.builder.TraversalBuilder;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Supplier;

public class LogicPlanProcessor extends AbstractGraphOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(LogicPlanProcessor.class);

    public LogicPlanProcessor(GaiaConfig config, GraphStoreService graphStore) {
        super(config, graphStore);
    }

    @Override
    protected GremlinExecutor.LifeCycle createLifeCycle(Context ctx, Supplier<GremlinExecutor> gremlinExecutorSupplier, BindingSupplier bindingsSupplier) {
        final RequestMessage msg = ctx.getRequestMessage();
        final Settings settings = ctx.getSettings();
        final Map<String, Object> args = msg.getArgs();
        final long seto = args.containsKey(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT) ?
                ((Number) args.get(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT)).longValue() : settings.scriptEvaluationTimeout;
        if (config.getGraphType() == GraphType.MAXGRAPH) {
            graphStore.updateSnapShotId();
        }
        return GremlinExecutor.LifeCycle.build()
                .scriptEvaluationTimeoutOverride(seto)
                .beforeEval(b -> {
                    try {
                        b.putAll(bindingsSupplier.get());
                    } catch (OpProcessorException ope) {
                        throw new RuntimeException(ope);
                    }
                })
                .transformResult(o -> {
                    if (o != null && o instanceof Traversal) {
                        applyStrategy((Traversal) o, config, graphStore);
                    }
                    return o;
                })
                .withResult(o -> {
                    if (o != null && o instanceof Traversal) {
                        long queryId = (long) queryIdMaker.getId(o);
                        TraversalBuilder traversalBuilder = new TraversalBuilder((Traversal.Admin) o)
                                .addConfig(PlanConfig.QUERY_ID, queryId)
                                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker((Traversal.Admin) o))
                                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId, config));
                        if (config.getGraphType() == GraphType.MAXGRAPH) {
                            traversalBuilder.addConfig(PlanConfig.SNAPSHOT_ID, Long.valueOf(graphStore.getSnapShotId()));
                        }
                        AbstractBuilder jobReqBuilder = new TraversalTranslator(traversalBuilder).translate();
                        String content = new String(jobReqBuilder.build().toByteArray(), StandardCharsets.ISO_8859_1);
                        AbstractGraphOpProcessor.writeResultList(ctx, Arrays.asList(content), ResponseStatusCode.SUCCESS);
                    }
                })
                .create();
    }

    @Override
    public String getName() {
        return "plan";
    }
}
