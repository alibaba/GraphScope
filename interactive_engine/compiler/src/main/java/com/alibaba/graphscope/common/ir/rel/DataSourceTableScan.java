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

import com.alibaba.graphscope.common.ir.rel.graph.AbstractBindableTableScan;
import com.alibaba.graphscope.common.ir.rel.type.AliasNameWithId;
import com.alibaba.graphscope.common.ir.rel.type.TableConfig;
import com.alibaba.graphscope.common.ir.rel.type.TargetGraph;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;
import java.util.Objects;

/**
 * Scan from the Graph DB using the input data from an external source
 */
public class DataSourceTableScan extends AbstractBindableTableScan implements DataSourceOperation {
    private final TargetGraph target;

    public DataSourceTableScan(
            GraphOptCluster cluster, List<RelHint> hints, RelNode source, TargetGraph target) {
        super(
                cluster,
                hints,
                source,
                new TableConfig(ImmutableList.of(target.getOptTable())),
                target.getAliasName(),
                AliasNameWithId.DEFAULT);
        this.target = Objects.requireNonNull(target);
        setSchemaType(target.getSingleSchemaType());
    }

    @Override
    public RelNode getExternalSource() {
        return input;
    }

    @Override
    public TargetGraph getTargetGraph() {
        return target;
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("target", target);
    }

    @Override
    public DataSourceTableScan copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DataSourceTableScan((GraphOptCluster) getCluster(), hints, sole(inputs), target);
    }
}
