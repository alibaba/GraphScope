/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.rel;

import com.alibaba.graphscope.common.ir.tools.AliasInference;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class GraphProcedureCall extends SingleRel {
    private final RexNode procedure;

    public GraphProcedureCall(
            RelOptCluster optCluster, RelTraitSet traitSet, RelNode input, RexNode procedure) {
        super(optCluster, traitSet, input);
        this.procedure = procedure;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("procedure", procedure);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }

    @Override
    public RelDataType deriveRowType() {
        Set<String> uniqueNameList = AliasInference.getUniqueAliasList(input, true);
        List<RelDataTypeField> reOrgFields =
                this.procedure.getType().getFieldList().stream()
                        .map(
                                k -> {
                                    // ensure the name is unique in the query
                                    String checkName =
                                            AliasInference.inferDefault(
                                                    k.getName(), uniqueNameList);
                                    uniqueNameList.add(checkName);
                                    return new RelDataTypeFieldImpl(
                                            checkName,
                                            ((GraphOptCluster) getCluster())
                                                    .getIdGenerator()
                                                    .generate(checkName),
                                            k.getType());
                                })
                        .collect(Collectors.toList());
        return new RelRecordType(reOrgFields);
    }

    @Override
    public GraphProcedureCall copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new GraphProcedureCall(getCluster(), traitSet, sole(inputs), procedure);
    }

    public RexNode getProcedure() {
        return procedure;
    }
}
