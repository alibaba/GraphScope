package com.alibaba.maxgraph.v2.frontend.compiler.cost;

import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeNode;
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
        return checkNotNull(computeTreeList.get(valueName), "Cant found compute for value " + valueName);
    }

    public double getComputeCost(Pair<String, String> computePair) {
        return computeCostList.get(computePair);
    }

    public double getValueNetworkCost(String value) {
        return valueNetworkCostList.get(value);
    }
}
