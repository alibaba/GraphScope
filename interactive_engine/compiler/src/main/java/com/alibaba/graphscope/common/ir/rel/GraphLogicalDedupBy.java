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

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Collections;
import java.util.List;

// this class is dedicated to supporting `dedup` operator in Gremlin, which is equivalent to the
// combination of `group` and `project` in SQL
public class GraphLogicalDedupBy extends SingleRel {
    private final List<RexNode> dedupByKeys;

    protected GraphLogicalDedupBy(
            RelOptCluster cluster, RelTraitSet traits, RelNode input, List<RexNode> dedupByKeys) {
        super(cluster, traits, input);
        this.dedupByKeys =
                ObjectUtils.requireNonEmpty(
                        dedupByKeys, "there should be at least one key in dedup by");
    }

    public static GraphLogicalDedupBy create(
            GraphOptCluster cluster, RelNode input, List<RexNode> dedupByKeys) {
        return new GraphLogicalDedupBy(cluster, RelTraitSet.createEmpty(), input, dedupByKeys);
    }

    public List<RexNode> getDedupByKeys() {
        return Collections.unmodifiableList(this.dedupByKeys);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("dedupByKeys", dedupByKeys);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new GraphLogicalDedupBy(
                this.getCluster(), traitSet, inputs.get(0), this.dedupByKeys);
    }
}
