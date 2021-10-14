/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.cost;

import com.alibaba.maxgraph.compiler.tree.TreeNode;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class CostMappingManager {
    private Map<String, String> valueParentList = Maps.newHashMap();
    private Map<Pair<String, String>, Double> computeCostList = Maps.newHashMap();
    private Map<String, TreeNode> computeTreeList = Maps.newHashMap();
    private Map<String, Double> valueNetworkCostList = Maps.newHashMap();

    public void addValueParent(String value, String parent) {
        this.valueParentList.put(value, parent);
    }

    public void addComputeCost(Pair<String, String> valueParent, double cost) {
        this.computeCostList.put(valueParent, cost);
    }

    public void addComputeTree(String value, TreeNode query) {
        this.computeTreeList.put(value, query);
    }

    public void addValueNetworkCost(String value, double network) {
        this.valueNetworkCostList.put(value, network);
    }

    public String getValueParent(String value) {
        return this.valueParentList.get(value);
    }

    public TreeNode getComputeTreeByValue(String valueName) {
        return checkNotNull(
                computeTreeList.get(valueName), "Cant found compute for value " + valueName);
    }

    public double getComputeCost(Pair<String, String> computePair) {
        return computeCostList.get(computePair);
    }

    public double getValueNetworkCost(String value) {
        return valueNetworkCostList.get(value);
    }
}
