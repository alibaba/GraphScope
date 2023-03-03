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

import com.alibaba.graphscope.common.ir.rel.type.TableConfig;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.commons.lang3.ObjectUtils;

import java.util.List;

public class GraphLogicalGetV extends AbstractBindableTableScan {

    protected GraphLogicalGetV(
            GraphOptCluster cluster, List<RelHint> hints, RelNode input, TableConfig tableConfig) {
        super(cluster, hints, input, tableConfig);
    }

    public static GraphLogicalGetV create(
            GraphOptCluster cluster, List<RelHint> hints, RelNode input, TableConfig tableConfig) {
        return new GraphLogicalGetV(cluster, hints, input, tableConfig);
    }

    public GraphOpt.GetV getOpt() {
        ObjectUtils.requireNonEmpty(hints);
        RelHint optHint = hints.get(0);
        ObjectUtils.requireNonEmpty(optHint.listOptions);
        return GraphOpt.GetV.valueOf(optHint.listOptions.get(0));
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("opt", getOpt());
    }
}
