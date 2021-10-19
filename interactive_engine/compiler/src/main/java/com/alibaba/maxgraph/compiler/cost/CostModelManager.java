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

import java.util.List;

public class CostModelManager {
    private CostGraph costGraph;
    private CostPath costPath;
    private int currentIndex = 0;

    public CostModelManager(CostGraph costGraph, CostPath costPath) {
        this.costGraph = costGraph;
        this.costPath = costPath;
    }

    public boolean hasBestPath() {
        return null != costPath;
    }

    public CostMappingManager getCostMappingManager() {
        return costGraph.getCostMappingManager();
    }

    public boolean processFieldValue(String fieldName) {
        RowFieldManager rowFieldManager = this.getPathFieldManager();
        if (null != rowFieldManager) {
            RowField rowField = rowFieldManager.getRowField();
            return rowField.getFieldList().contains(fieldName) ||
                    rowField.getFieldList().contains(CostUtils.buildValueName(fieldName));
        } else {
            return false;
        }
    }

    public RowFieldManager getPathFieldManager() {
        if (null != costPath) {
            return this.costPath.getRowFieldManager(currentIndex);
        } else {
            return null;
        }
    }

    public void stepNextIndex() {
        this.currentIndex++;
    }

    public List<CostPath> getPathList() {
        return costGraph.getCostPathList();
    }
}
