package com.alibaba.maxgraph.v2.frontend.compiler.strategy.tree;

import com.alibaba.maxgraph.proto.v2.RequirementType;
import com.alibaba.maxgraph.v2.frontend.compiler.strategy.GraphTreeStrategy;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.AndTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.BaseTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.NodeType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.RepeatTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.UnionTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.WherePredicateTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.TreeNodeUtils;

import java.util.Map;
import java.util.Set;

public class LabelPushDownStrategy implements GraphTreeStrategy {
    public static final GraphTreeStrategy INSTANCE = new LabelPushDownStrategy();

    @Override
    public void apply(TreeManager treeManager) {
        BaseTreeNode treeNode = (BaseTreeNode) TreeNodeUtils.getSourceTreeNode(treeManager.getTreeLeaf());
        while (null != treeNode) {
            Map<RequirementType, Object> afterRequirementList = treeNode.getAfterRequirementList();
            BaseTreeNode outputNode = (BaseTreeNode) treeNode.getOutputNode();
            if (null == outputNode) {
                break;
            }
            Set<String> labelList = (Set<String>) afterRequirementList.remove(RequirementType.LABEL_START);
            if (null != labelList) {
                if (outputNode instanceof WherePredicateTreeNode
                        || outputNode instanceof UnionTreeNode
                        || outputNode instanceof RepeatTreeNode
                        || outputNode instanceof AndTreeNode
                        || outputNode.getNodeType() == NodeType.AGGREGATE) {
                    afterRequirementList.put(RequirementType.LABEL_START, labelList);
                } else if (outputNode.getNodeType() != NodeType.FILTER) {
                    outputNode.getBeforeRequirementList().put(RequirementType.LABEL_START, labelList);
                } else {
                    while (outputNode.getNodeType() == NodeType.FILTER) {
                        BaseTreeNode nextOutputNode = (BaseTreeNode) outputNode.getOutputNode();
                        if (null == nextOutputNode || outputNode instanceof WherePredicateTreeNode) {
                            outputNode.getAfterRequirementList().put(RequirementType.LABEL_START, labelList);
                            break;
                        } else if (nextOutputNode.getNodeType() != NodeType.FILTER) {
                            nextOutputNode.getBeforeRequirementList().put(RequirementType.LABEL_START, labelList);
                            break;
                        } else {
                            outputNode = nextOutputNode;
                        }
                    }
                }
            }
            treeNode = (BaseTreeNode) treeNode.getOutputNode();
        }
    }
}
