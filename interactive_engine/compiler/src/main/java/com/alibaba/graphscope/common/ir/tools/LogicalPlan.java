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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * logical plan for a query which can be a regular query or a procedure call
 */
public class LogicalPlan {
    private @Nullable RelNode regularQuery;
    private @Nullable RexNode procedureCall;
    private boolean returnEmpty;

    public LogicalPlan(RelNode regularQuery, boolean returnEmpty) {
        this.regularQuery = Objects.requireNonNull(regularQuery);
        this.returnEmpty = returnEmpty;
    }

    public LogicalPlan(RexNode procedureCall) {
        this.procedureCall = Objects.requireNonNull(procedureCall);
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
}
