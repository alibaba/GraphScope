package com.alibaba.maxgraph.v2.frontend.compiler.cost.statistics;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Manager the label of the output of nodes
 */
public class NodeLabelManager {
    private List<NodeLabelList> nodeLabelList = Lists.newArrayList();

    public void addNodeLabelList(NodeLabelList nodeLabel) {
        this.nodeLabelList.add(nodeLabel);
    }

    public List<NodeLabelList> getNodeLabelList() {
        return nodeLabelList;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("nodeLabelList", nodeLabelList)
                .toString();
    }
}
