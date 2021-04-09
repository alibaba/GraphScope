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
