package com.alibaba.maxgraph.v2.frontend.compiler.cost;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class CostPath {
    private List<RowFieldManager> pathList;

    public CostPath(List<RowFieldManager> pathList) {
        this.pathList = pathList;
    }

    public static CostPath fromCostPath(CostPath costPath) {
        List<RowFieldManager> pathList = Lists.newArrayList(costPath.pathList);

        return new CostPath(pathList);
    }

    public RowFieldManager getRowFieldManager(int index) {
        if (index < pathList.size()) {
            return pathList.get(index);
        } else {
            return null;
        }
    }

    public void addRowFieldManager(RowFieldManager rowField) {
        this.pathList.add(rowField);
    }

    public RowFieldManager getLastRowFieldManager() {
        return pathList.get(pathList.size() - 1);
    }

    public double computeCost(List<Double> stepCountList,
                              List<Double> shuffleThresholdList,
                              CostMappingManager costMappingManager) {
        double totalCost = 0.0;
        int stepIndex = 0;
        for (int i = 0; i < pathList.size(); i++) {
            RowFieldManager fieldManager = pathList.get(i);
            RowFieldManager parentManager = fieldManager.getParent();
            double stepTotal = 0.0;
            for (String field : fieldManager.getRowField().getFieldList()) {
                if (null == parentManager || !parentManager.getRowField().getFieldList().contains(field)) {
                    String parentValue = costMappingManager.getValueParent(field);
                    stepTotal += (stepCountList.get(stepIndex) * costMappingManager.getComputeCost(
                            Pair.of(StringUtils.isEmpty(parentValue) ? field : parentValue, field))) *
                            (StringUtils.isEmpty(parentValue) ? 1.0 : shuffleThresholdList.get(stepIndex));
                }
                if (i < pathList.size() - 1) {
                    stepTotal += (stepCountList.get(stepIndex) * costMappingManager.getValueNetworkCost(field));
                }
            }
            totalCost += stepTotal;
            stepIndex++;
        }
        return totalCost;
    }

    @Override
    public String toString() {
        return StringUtils.join(pathList, "->");
    }
}
