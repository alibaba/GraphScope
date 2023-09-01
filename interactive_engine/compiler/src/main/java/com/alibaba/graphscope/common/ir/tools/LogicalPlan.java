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

package com.alibaba.graphscope.common.ir.tools;

import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * logical plan for a query which can be a regular query or a procedure call
 */
public class LogicalPlan {
    private @Nullable RelNode regularQuery;
    private @Nullable RexNode procedureCall;
    private boolean returnEmpty;

    private final List<StoredProcedureMeta.Parameter> dynamicParams;

    public LogicalPlan(RelNode regularQuery) {
        this(regularQuery, ImmutableList.of());
    }

    public LogicalPlan(RelNode regularQuery, List<StoredProcedureMeta.Parameter> dynamicParams) {
        this.regularQuery = Objects.requireNonNull(regularQuery);
        this.returnEmpty = returnEmpty(this.regularQuery);
        this.dynamicParams = Objects.requireNonNull(dynamicParams);
    }

    public LogicalPlan(RexNode procedureCall) {
        this.procedureCall = Objects.requireNonNull(procedureCall);
        this.dynamicParams = ImmutableList.of();
    }

    public @Nullable RelNode getRegularQuery() {
        return regularQuery;
    }

    public @Nullable RexNode getProcedureCall() {
        return procedureCall;
    }

    public boolean isReturnEmpty() {
        return returnEmpty;
    }

    public String explain() {
        if (this.regularQuery != null) {
            return this.regularQuery.explain();
        } else if (this.procedureCall != null) {
            return this.procedureCall.toString();
        } else {
            return StringUtils.EMPTY;
        }
    }

    private boolean returnEmpty(RelNode relNode) {
        List<RelNode> inputs = Lists.newArrayList(relNode);
        while (!inputs.isEmpty()) {
            RelNode cur = inputs.remove(0);
            if (cur instanceof LogicalValues) {
                return true;
            }
            if (cur instanceof GraphLogicalSingleMatch) {
                GraphLogicalSingleMatch match = (GraphLogicalSingleMatch) cur;
                if (returnEmpty(match.getSentence())) {
                    return true;
                }
            } else if (cur instanceof GraphLogicalMultiMatch) {
                GraphLogicalMultiMatch match = (GraphLogicalMultiMatch) cur;
                for (RelNode sentence : match.getSentences()) {
                    if (returnEmpty(sentence)) {
                        return true;
                    }
                }
            }
            inputs.addAll(cur.getInputs());
        }
        return false;
    }

    public @Nullable RelDataType getOutputType() {
        if (regularQuery != null) {
            return Utils.getOutputType(regularQuery);
        } else if (procedureCall != null) {
            return procedureCall.getType();
        } else {
            return null;
        }
    }

    public List<StoredProcedureMeta.Parameter> getDynamicParams() {
        return Collections.unmodifiableList(dynamicParams);
    }
}
