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

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

public class GraphPattern extends AbstractRelNode {
    private final Pattern pattern;
    private double rowCount = 0.0d;

    public GraphPattern(RelOptCluster cluster, RelTraitSet traitSet, Pattern pattern) {
        super(cluster, traitSet);
        this.pattern = pattern;
        recomputeDigest();
    }

    public Pattern getPattern() {
        return pattern;
    }

    @Override
    public RelDataType deriveRowType() {
        return getCluster().getTypeFactory().createSqlType(SqlTypeName.ANY);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return super.accept(shuttle);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("pattern", pattern);
    }

    public void setRowCount(double rowCount) {
        this.rowCount = rowCount;
    }

    public double getRowCount() {
        return rowCount;
    }
}
