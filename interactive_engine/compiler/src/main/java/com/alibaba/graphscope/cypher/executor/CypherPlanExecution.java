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

package com.alibaba.graphscope.cypher.executor;

import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.tools.GraphPlanExecutor;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.cypher.result.CypherRecordParser;
import com.alibaba.graphscope.cypher.result.CypherRecordProcessor;
import com.alibaba.graphscope.gremlin.plugin.QueryStatusCallback;

import org.neo4j.fabric.stream.StatementResults;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;

public class CypherPlanExecution implements StatementResults.SubscribableExecution {
    private final GraphPlanner.Summary planSummary;
    private final QueryTimeoutConfig timeoutConfig;
    private final QueryStatusCallback statusCallback;
    private final IrMeta irMeta;
    private final GraphPlanExecutor innerExecutor;

    public CypherPlanExecution(
            GraphPlanner.Summary planSummary,
            QueryTimeoutConfig timeoutConfig,
            QueryStatusCallback statusCallback,
            IrMeta irMeta,
            GraphPlanExecutor innerExecutor) {
        this.planSummary = planSummary;
        this.timeoutConfig = timeoutConfig;
        this.statusCallback = statusCallback;
        this.irMeta = irMeta;
        this.innerExecutor = innerExecutor;
    }

    @Override
    public QueryExecution subscribe(QuerySubscriber querySubscriber) {
        try {
            CypherRecordProcessor recordProcessor =
                    new CypherRecordProcessor(
                            new CypherRecordParser(planSummary.getLogicalPlan().getOutputType()),
                            querySubscriber,
                            timeoutConfig,
                            statusCallback);
            innerExecutor.execute(planSummary, irMeta, recordProcessor);
            return recordProcessor;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
