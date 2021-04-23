package com.alibaba.maxgraph.v2.frontend.compiler.optimizer;

public class OptimizeConfig {
    private boolean chainOptimize;
    private boolean chainGlobalAggregate;
    private boolean chainBinary;
    private boolean aggregateJoinOptimize;

    public OptimizeConfig() {
        this.chainOptimize = false;
        this.chainGlobalAggregate = false;
        this.chainBinary = false;
        this.aggregateJoinOptimize = false;
    }

    public boolean isChainOptimize() {
        return chainOptimize;
    }

    public void setChainOptimize(boolean chainOptimize) {
        this.chainOptimize = chainOptimize;
    }

    public boolean isChainGlobalAggregate() {
        return chainGlobalAggregate;
    }

    public void setChainGlobalAggregate(boolean chainGlobalAggregate) {
        this.chainGlobalAggregate = chainGlobalAggregate;
    }

    public boolean isChainBinary() {
        return chainBinary;
    }

    public void setChainBinary(boolean chainBinary) {
        this.chainBinary = chainBinary;
    }

    public boolean isAggregateJoinOptimize() {
        return aggregateJoinOptimize;
    }

    public void setAggregateJoinOptimize(boolean aggregateJoinOptimize) {
        this.aggregateJoinOptimize = aggregateJoinOptimize;
    }
}
