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

package com.alibaba.graphscope.common.ir.runtime;

import com.alibaba.graphscope.common.ir.runtime.proto.Utils;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gaia.proto.StoredProcedure;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

public class ProcedurePhysicalBuilder extends PhysicalBuilder {
    private final StoredProcedure.Query.Builder builder;

    public ProcedurePhysicalBuilder(LogicalPlan logicalPlan) {
        super(logicalPlan);
        this.builder = StoredProcedure.Query.newBuilder();
        RexCall procedureCall = (RexCall) logicalPlan.getProcedureCall();
        setStoredProcedureName(procedureCall, builder);
        setStoredProcedureArgs(procedureCall, builder);
    }

    private void setStoredProcedureName(
            RexCall procedureCall, StoredProcedure.Query.Builder builder) {
        SqlOperator operator = procedureCall.getOperator();
        builder.setQueryName(Common.NameOrId.newBuilder().setName(operator.getName()).build());
    }

    private void setStoredProcedureArgs(
            RexCall procedureCall, StoredProcedure.Query.Builder builder) {
        List<RexNode> operands = procedureCall.getOperands();
        for (int i = 0; i < operands.size(); ++i) {
            builder.addArguments(
                    StoredProcedure.Argument.newBuilder()
                            // param name is omitted
                            .setParamInd(i)
                            .setValue(Utils.protoValue((RexLiteral) operands.get(i)))
                            .build());
        }
    }

    @Override
    public PhysicalPlan build() {
        return new PhysicalPlan(this.builder.build().toByteArray(), explain());
    }

    @Override
    public void close() throws Exception {}

    private String explain() {
        try {
            return JsonFormat.printer().print(this.builder);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
