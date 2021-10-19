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
package com.alibaba.maxgraph.compiler.cost;

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
