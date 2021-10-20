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
import com.alibaba.maxgraph.compiler.strategy.GraphTreeStrategy;
import com.alibaba.maxgraph.compiler.tree.AndTreeNode;
import com.alibaba.maxgraph.compiler.tree.BaseTreeNode;
import com.alibaba.maxgraph.compiler.tree.NodeType;
import com.alibaba.maxgraph.compiler.tree.RepeatTreeNode;
import com.alibaba.maxgraph.compiler.tree.TreeManager;
import com.alibaba.maxgraph.compiler.tree.UnionTreeNode;
import com.alibaba.maxgraph.compiler.tree.WherePredicateTreeNode;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;

import java.util.Map;
import java.util.Set;

public class LabelPushDownStrategy implements GraphTreeStrategy {
    public static final GraphTreeStrategy INSTANCE = new LabelPushDownStrategy();

    @Override
    public void apply(TreeManager treeManager) {
        while (true) {
            BaseTreeNode treeNode = (BaseTreeNode) TreeNodeUtils.getSourceTreeNode(treeManager.getTreeLeaf());
            boolean labelOptimizeFlag = false;
            while (true) {
                Map<QueryFlowOuterClass.RequirementType, Object> afterRequirementList = treeNode.getAfterRequirementList();
                BaseTreeNode outputNode = (BaseTreeNode) treeNode.getOutputNode();
                if (null != outputNode &&
                        !(outputNode instanceof RepeatTreeNode) &&
                        !(outputNode instanceof UnionTreeNode) &&
                        !(outputNode instanceof AndTreeNode) &&
                        !(outputNode instanceof WherePredicateTreeNode) &&
                        outputNode.getNodeType() != NodeType.AGGREGATE) {
                    Set<String> labelList = (Set<String>) afterRequirementList.remove(QueryFlowOuterClass.RequirementType.LABEL_START);
                    if (null != labelList) {
                        labelOptimizeFlag = true;
                        outputNode.getBeforeRequirementList().put(QueryFlowOuterClass.RequirementType.LABEL_START, labelList);
                    }
                }
                if (outputNode == null) {
                    break;
                }
                treeNode = outputNode;
            }
            if (!labelOptimizeFlag) {
                break;
            }
        }
    }
}
