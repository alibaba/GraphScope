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

package com.alibaba.graphscope.common.ir.rel;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.List;

/**
 * extend {@link Project} in calcite to implement append=true/false
 */
public class GraphLogicalProject extends Project {
    private boolean isAppend;

    protected GraphLogicalProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType,
            boolean isAppend) {
        super(cluster, traits, hints, input, projects, rowType);
        this.isAppend = isAppend;
    }

    public static GraphLogicalProject create(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType dataType,
            boolean isAppend) {
        return new GraphLogicalProject(
                cluster, RelTraitSet.createEmpty(), hints, input, projects, dataType, isAppend);
    }

    public boolean isAppend() {
        return isAppend;
    }

    @Override
    public Project copy(
            RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new GraphLogicalProject(
                getCluster(),
                traitSet,
                ImmutableList.of(),
                input,
                projects,
                rowType,
                this.isAppend);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        List<RexNode> exps = shuttle.apply(this.exps);
        if (this.exps == exps) {
            return this;
        } else {
            // if exps are changed, we need to update the row type.
            // here we override the original method to innovate and define our own type info for the
            // project
            Preconditions.checkArgument(
                    exps.size() == this.exps.size(),
                    "rex shuttle should not change the size of exps");
            List<RelDataTypeField> oldFields = this.getRowType().getFieldList();
            List<RelDataTypeField> newFields = Lists.newArrayList();
            for (int i = 0; i < exps.size(); ++i) {
                RelDataTypeField oldField = oldFields.get(i);
                newFields.add(
                        new RelDataTypeFieldImpl(
                                oldField.getName(), oldField.getIndex(), exps.get(i).getType()));
            }
            return this.copy(
                    this.traitSet,
                    this.input,
                    exps,
                    new RelRecordType(StructKind.FULLY_QUALIFIED, newFields));
        }
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("isAppend", isAppend);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }
}
