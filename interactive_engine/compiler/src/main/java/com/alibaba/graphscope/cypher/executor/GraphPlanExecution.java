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

import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.cypher.result.CypherRecordParser;
import com.alibaba.graphscope.cypher.result.CypherRecordProcessor;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.neo4j.fabric.stream.StatementResults;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;

import java.util.ArrayList;
import java.util.List;

public class GraphPlanExecution<C> implements StatementResults.SubscribableExecution {
    private final ExecutionClient<C> client;
    private final GraphPlanner.Summary planSummary;

    public GraphPlanExecution(ExecutionClient<C> client, GraphPlanner.Summary planSummary) {
        this.client = client;
        this.planSummary = planSummary;
    }

    @Override
    public QueryExecution subscribe(QuerySubscriber querySubscriber) {
        try {
            ExecutionRequest request =
                    new ExecutionRequest(
                            this.planSummary.getId(),
                            this.planSummary.getName(),
                            this.planSummary.getPhysicalBuilder());
            CypherRecordProcessor recordProcessor =
                    new CypherRecordProcessor(
                            new CypherRecordParser(getOutputType(planSummary.getLogicalPlan())),
                            querySubscriber);
            this.client.submit(request, recordProcessor);
            return recordProcessor;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private RelDataType getOutputType(LogicalPlan logicalPlan) {
        if (logicalPlan.getRegularQuery() != null) {
            List<RelNode> inputs = Lists.newArrayList(logicalPlan.getRegularQuery());
            List<RelDataTypeField> outputFields = new ArrayList<>();
            while (!inputs.isEmpty()) {
                RelNode cur = inputs.remove(0);
                outputFields.addAll(cur.getRowType().getFieldList());
                if (AliasInference.removeAlias(cur)) {
                    break;
                }
                inputs.addAll(cur.getInputs());
            }
            return new RelRecordType(StructKind.FULLY_QUALIFIED, outputFields);
        } else {
            return logicalPlan.getProcedureCall().getType();
        }
    }
}
