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

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexNode;

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
            RelDataType rowType) {
        super(cluster, traits, hints, input, projects, rowType);
    }

    protected GraphLogicalProject(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType dataType,
            boolean isAppend) {
        this(cluster, RelTraitSet.createEmpty(), hints, input, projects, dataType);
        this.isAppend = isAppend;
    }

    public static GraphLogicalProject create(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType dataType,
            boolean isAppend) {
        return new GraphLogicalProject(cluster, hints, input, projects, dataType, isAppend);
    }

    public boolean isAppend() {
        return isAppend;
    }

    @Override
    public Project copy(
            RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new GraphLogicalProject(
                getCluster(), traitSet, ImmutableList.of(), input, projects, rowType);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("isAppend", isAppend);
    }
}
