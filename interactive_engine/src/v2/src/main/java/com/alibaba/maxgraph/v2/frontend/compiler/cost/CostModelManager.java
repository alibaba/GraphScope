package com.alibaba.maxgraph.v2.frontend.compiler.cost;

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
