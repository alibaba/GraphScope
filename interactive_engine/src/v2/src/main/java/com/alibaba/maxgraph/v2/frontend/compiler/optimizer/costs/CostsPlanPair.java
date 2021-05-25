/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.v2.frontend.compiler.optimizer.costs;

import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;

public class CostsPlanPair implements Comparable<CostsPlanPair> {
    private Costs costs;
    private LogicalSubQueryPlan queryPlan;

    private CostsPlanPair(Costs costs, LogicalSubQueryPlan queryPlan) {
        this.costs = costs;
        this.queryPlan = queryPlan;
    }

    public static CostsPlanPair of(Costs costs, LogicalSubQueryPlan queryPlan) {
        return new CostsPlanPair(costs, queryPlan);
    }

    public LogicalSubQueryPlan getQueryPlan() {
        return queryPlan;
    }

    @Override
    public int compareTo(CostsPlanPair o) {
        return costs.compareTo(o.costs);
    }
}
