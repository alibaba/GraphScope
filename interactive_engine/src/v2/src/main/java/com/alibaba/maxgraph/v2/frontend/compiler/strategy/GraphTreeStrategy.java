package com.alibaba.maxgraph.v2.frontend.compiler.strategy;

import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeManager;

public interface GraphTreeStrategy {
    void apply(TreeManager treeManager);
}
