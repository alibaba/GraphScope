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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class CostGraph {
    private static final Logger logger = LoggerFactory.getLogger(CostGraph.class);

    private CostMappingManager costMappingManager;
    private LinkedList<CostRow> costRowList;

    public CostGraph(CostMappingManager costMappingManager) {
        this.costMappingManager = costMappingManager;
        this.costRowList = Lists.newLinkedList();
    }

    public void addRow(CostRow costRow) {
        this.costRowList.add(costRow);
    }

    public void addFirstRow(CostRow costRow) {
        costRowList.addFirst(costRow);
    }

    public void mergeCostGraph(CostGraph costGraph) {
        List<CostRow> costRowList = costGraph.getCostRowList();
        int rowCount = Math.max(this.costRowList.size(), costRowList.size());
        for (int i = 0; i < rowCount; i++) {
            if (i < this.costRowList.size() && i < costRowList.size()) {
                CostRow currentCostRow = this.costRowList.get(i);
                CostRow otherCostRow = costRowList.get(i);
                currentCostRow.mergeCostRow(otherCostRow);
            } else if (i < costRowList.size()) {
                CostRow currentCostRow = new CostRow(Lists.newArrayList());
                CostRow otherCostRow = costRowList.get(i);
                currentCostRow.mergeCostRow(otherCostRow);
                this.costRowList.add(currentCostRow);
            }
        }
    }

    private List<CostRow> getCostRowList() {
        return this.costRowList;
    }

    public List<CostPath> getCostPathList() {
        List<CostPath> pathList = Lists.newArrayList();
        for (CostRow costRow : costRowList) {
            if (pathList.isEmpty()) {
                if (costRow.getFieldList().isEmpty()) {
                    pathList.add(new CostPath(Lists.newArrayList(new RowFieldManager(RowField.emptyRowField(), costRow.getBirthFieldList()))));
                } else {
                    for (RowField rowField : costRow.getFieldList()) {
                        pathList.add(new CostPath(Lists.newArrayList(new RowFieldManager(rowField, costRow.getBirthFieldList()))));
                    }
                }
            } else {
                List<CostPath> currPathList = Lists.newArrayList();
                for (CostPath costPath : pathList) {
                    if (costRow.getFieldList().isEmpty()) {
                        CostPath currPath = CostPath.fromCostPath(costPath);
                        currPath.addRowFieldManager(new RowFieldManager(RowField.emptyRowField(), costRow.getBirthFieldList()));
                        currPathList.add(currPath);
                    } else {
                        for (RowField rowField : costRow.getFieldList()) {
                            CostPath currPath = CostPath.fromCostPath(costPath);
                            RowFieldManager lastRowFiend = currPath.getLastRowFieldManager();
                            boolean valid = true;
                            for (String field : rowField.getFieldList()) {
                                String parent = costMappingManager.getValueParent(field);
                                if (!(costRow.getBirthFieldList().contains(field) ||
                                        lastRowFiend.getRowField().getFieldList().contains(field) ||
                                        (!StringUtils.isEmpty(parent) && lastRowFiend.getRowField().getFieldList().contains(parent)))) {
                                    valid = false;
                                    break;
                                }
                            }
                            if (valid) {
                                currPath.addRowFieldManager(new RowFieldManager(rowField, lastRowFiend, costRow.getBirthFieldList()));
                                currPathList.add(currPath);
                            }
                        }
                    }
                }
                pathList.clear();
                pathList.addAll(currPathList);
            }
        }

        return pathList;
    }

    public CostPath computePath(List<Double> stepCountList, List<Double> shuffleThresholdList) {
        List<CostPath> costPathList = this.getCostPathList();
        if (costPathList.size() <= 1) {
            return null;
        }

        CostPath bestPath = costPathList.get(0);
        double costValue = bestPath.computeCost(stepCountList, shuffleThresholdList, costMappingManager);
        for (int i = 0; i < costPathList.size(); i++) {
            CostPath currPath = costPathList.get(i);
            double currCostValue = currPath.computeCost(stepCountList, shuffleThresholdList, costMappingManager);
            if (currCostValue < costValue) {
                bestPath = currPath;
                costValue = currCostValue;
            }
        }
        logger.info("best path=>" + bestPath.toString());

        return bestPath;
    }

    public CostMappingManager getCostMappingManager() {
        return this.costMappingManager;
    }

    public void clear() {
        this.costRowList.clear();
    }
}
