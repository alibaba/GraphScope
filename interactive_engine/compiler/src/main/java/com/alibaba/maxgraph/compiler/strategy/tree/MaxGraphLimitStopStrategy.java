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
package com.alibaba.maxgraph.compiler.strategy.tree;

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.common.util.CompilerConstant;
import com.alibaba.maxgraph.compiler.strategy.GraphTreeStrategy;
import com.alibaba.maxgraph.compiler.tree.RangeGlobalTreeNode;
import com.alibaba.maxgraph.compiler.tree.TreeManager;
import com.alibaba.maxgraph.compiler.tree.TreeNode;
import com.alibaba.maxgraph.compiler.tree.UnaryTreeNode;

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
                    QueryFlowOuterClass.InputBatchLevel.VerySmall.name());
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
