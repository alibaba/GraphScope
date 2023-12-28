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

package com.alibaba.graphscope.common.ir.rel.graph;

import com.alibaba.graphscope.common.ir.rel.GraphShuttle;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.commons.lang3.ObjectUtils;

import java.util.List;

public class GraphPhysicalGetV extends SingleRel {
    private final GraphOpt.PhysicalGetVOpt physicalOpt;
    private final GraphLogicalGetV fusedGetV;

    protected GraphPhysicalGetV(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphLogicalGetV fusedGetV,
            GraphOpt.PhysicalGetVOpt physicalOpt) {
        super(cluster, RelTraitSet.createEmpty(), input);
        this.physicalOpt = physicalOpt;
        this.fusedGetV = fusedGetV;
    }

    public static GraphPhysicalGetV create(
            RelNode input, GraphLogicalGetV innerGetV, GraphOpt.PhysicalGetVOpt physicalOpt) {
        return new GraphPhysicalGetV(
                (GraphOptCluster) innerGetV.getCluster(),
                innerGetV.getHints(),
                input,
                innerGetV,
                physicalOpt);
    }

    public GraphOpt.PhysicalGetVOpt getPhysicalOpt() {
        return this.physicalOpt;
    }

    public GraphLogicalGetV getFusedGetV() {
        return fusedGetV;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("tableConfig", fusedGetV.tableConfig)
                .item("alias", AliasInference.SIMPLE_NAME(fusedGetV.getAliasName()))
                .itemIf(
                        "startAlias",
                        fusedGetV.getStartAlias(),
                        fusedGetV.getStartAlias().getAliasName() != AliasInference.DEFAULT_NAME)
                .itemIf(
                        "fusedFilter",
                        fusedGetV.getFilters(),
                        !ObjectUtils.isEmpty(fusedGetV.getFilters()))
                .item("opt", fusedGetV.getOpt())
                .item("physicalOpt", getPhysicalOpt());
    }

    @Override
    public GraphPhysicalGetV copy(RelTraitSet traitSet, List<RelNode> inputs) {
        GraphPhysicalGetV copy =
                new GraphPhysicalGetV(
                        (GraphOptCluster) getCluster(),
                        fusedGetV.getHints(),
                        inputs.get(0),
                        fusedGetV,
                        getPhysicalOpt());
        return copy;
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }
}
