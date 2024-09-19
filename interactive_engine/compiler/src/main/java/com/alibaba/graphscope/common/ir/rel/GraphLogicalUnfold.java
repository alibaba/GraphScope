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
import com.alibaba.graphscope.common.ir.tools.Utils;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.stream.Collectors;

public class GraphLogicalUnfold extends SingleRel {
    private final RexNode key;
    private final String aliasName;
    private final int aliasId;

    public GraphLogicalUnfold(
            GraphOptCluster cluster, RelNode input, RexNode key, @Nullable String aliasName) {
        super(cluster, RelTraitSet.createEmpty(), input);
        this.key = key;
        this.aliasName =
                AliasInference.inferDefault(
                        aliasName, AliasInference.getUniqueAliasList(input, true));
        this.aliasId = cluster.getIdGenerator().generate(this.aliasName);
    }

    public RexNode getUnfoldKey() {
        return this.key;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("key", key).item("alias", aliasName);
    }

    @Override
    public RelDataType deriveRowType() {
        RelDataType inputOutputType = Utils.getOutputType(input);
        List<RelDataTypeField> fields =
                inputOutputType.getFieldList().stream()
                        .filter(k -> k.getIndex() != AliasInference.DEFAULT_ID)
                        .collect(Collectors.toList());
        fields.add(
                new RelDataTypeFieldImpl(
                        this.aliasName, this.aliasId, key.getType().getComponentType()));
        return new RelRecordType(StructKind.FULLY_QUALIFIED, fields);
    }

    @Override
    public GraphLogicalUnfold copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new GraphLogicalUnfold(
                (GraphOptCluster) getCluster(), inputs.get(0), this.key, this.aliasName);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }

    public String getAliasName() {
        return this.aliasName;
    }

    public int getAliasId() {
        return this.aliasId;
    }
}
