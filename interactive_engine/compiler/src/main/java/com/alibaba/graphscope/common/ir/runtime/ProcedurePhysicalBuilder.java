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

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.runtime.proto.RexToProtoConverter;
import com.alibaba.graphscope.common.ir.runtime.proto.Utils;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.gaia.proto.StoredProcedure;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

public class ProcedurePhysicalBuilder extends PhysicalBuilder {
    private final StoredProcedure.Query.Builder builder;

    public ProcedurePhysicalBuilder(Configs configs, IrMeta irMeta, LogicalPlan logicalPlan) {
        super(logicalPlan);
        this.builder =
                Utils.protoProcedure(
                        logicalPlan.getProcedureCall(),
                        new RexToProtoConverter(
                                true,
                                irMeta.getSchema().isColumnId(),
                                GraphPlanner.rexBuilderFactory.apply(configs)));
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
