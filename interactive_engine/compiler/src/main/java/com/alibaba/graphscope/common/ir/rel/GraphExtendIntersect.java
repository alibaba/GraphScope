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

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueExtendIntersectEdge;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class GraphExtendIntersect extends SingleRel {
    private final GlogueExtendIntersectEdge glogueEdge;

    public GraphExtendIntersect(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            GlogueExtendIntersectEdge glogueEdge) {
        super(cluster, traits, input);
        this.glogueEdge = glogueEdge;
    }

    public GlogueExtendIntersectEdge getGlogueEdge() {
        return glogueEdge;
    }

    @Override
    public GraphExtendIntersect copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new GraphExtendIntersect(
                getCluster(), traitSet, inputs.get(0), this.getGlogueEdge());
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return super.accept(shuttle);
    }

    @Override
    public RelDataType deriveRowType() {
        return getCluster().getTypeFactory().createSqlType(SqlTypeName.ANY);
    }

    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("glogueEdge", glogueEdge);
    }
}
