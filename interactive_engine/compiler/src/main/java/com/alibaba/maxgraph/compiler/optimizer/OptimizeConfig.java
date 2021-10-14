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
package com.alibaba.maxgraph.compiler.optimizer;

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
