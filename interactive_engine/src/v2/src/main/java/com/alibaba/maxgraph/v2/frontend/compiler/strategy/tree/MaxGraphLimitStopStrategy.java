package com.alibaba.maxgraph.v2.frontend.compiler.strategy.tree;

import com.alibaba.maxgraph.proto.v2.InputBatchLevel;
import com.alibaba.maxgraph.v2.frontend.compiler.strategy.GraphTreeStrategy;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.RangeGlobalTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.UnaryTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.CompilerConstant;

public class MaxGraphLimitStopStrategy implements GraphTreeStrategy {
    public static final GraphTreeStrategy INSTANCE = new MaxGraphLimitStopStrategy();

    @Override
    public void apply(TreeManager treeManager) {
        TreeNode currNode = treeManager.getTreeLeaf();
        boolean limitStopFlag = false;
        while (currNode instanceof UnaryTreeNode) {
            if (currNode instanceof RangeGlobalTreeNode) {
                RangeGlobalTreeNode rangeGlobalTreeNode = (RangeGlobalTreeNode) currNode;
                if (rangeGlobalTreeNode.getLow() == 0) {
                    limitStopFlag = true;
                }
                break;
            }
            currNode = ((UnaryTreeNode) currNode).getInputNode();
        }

        if (limitStopFlag) {
            treeManager.getQueryConfig().addProperty(
                    CompilerConstant.QUERY_SCHEDULE_GRANULARITY,
                    InputBatchLevel.VerySmall.name());
            currNode.enableGlobalStop();
            TreeNode node = ((UnaryTreeNode) currNode).getInputNode();
            while (node instanceof UnaryTreeNode) {
                node.enableGlobalFilter();
                node = ((UnaryTreeNode) node).getInputNode();
            }
            node.enableGlobalFilter();
        }

    }
}
