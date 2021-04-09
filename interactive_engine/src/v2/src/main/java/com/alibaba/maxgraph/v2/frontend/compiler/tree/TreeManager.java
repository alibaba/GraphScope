package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.RangeLimit;
import com.alibaba.maxgraph.proto.v2.VariantType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.common.ExecuteMode;
import com.alibaba.maxgraph.v2.frontend.compiler.cost.CostGraph;
import com.alibaba.maxgraph.v2.frontend.compiler.cost.CostModelManager;
import com.alibaba.maxgraph.v2.frontend.compiler.cost.CostPath;
import com.alibaba.maxgraph.v2.frontend.compiler.cost.CostUtils;
import com.alibaba.maxgraph.v2.frontend.compiler.cost.statistics.CostDataStatistics;
import com.alibaba.maxgraph.v2.frontend.compiler.cost.statistics.NodeLabelList;
import com.alibaba.maxgraph.v2.frontend.compiler.cost.statistics.NodeLabelManager;
import com.alibaba.maxgraph.v2.frontend.compiler.strategy.GraphTreeStrategy;
import com.alibaba.maxgraph.v2.frontend.compiler.strategy.tree.LabelPushDownStrategy;
import com.alibaba.maxgraph.v2.frontend.compiler.strategy.tree.MaxGraphLimitStopStrategy;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.PropertyNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.source.SourceDelegateNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.source.SourceTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.source.SourceVertexTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.EdgeValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.CompilerConstant;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.CompilerUtils;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.TreeNodeUtils;
import com.google.common.collect.Lists;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TreeManager {
    private static final Logger logger = LoggerFactory.getLogger(TreeManager.class);

    private List<GraphTreeStrategy> graphTreeStrategyList = Lists.newArrayList(
            LabelPushDownStrategy.INSTANCE,
            MaxGraphLimitStopStrategy.INSTANCE);

    private TreeNode treeLeaf;
    private GraphSchema schema;
    private TreeNodeLabelManager labelManager;
    private Configuration configuration;

    public TreeManager(TreeNode treeLeaf,
                       GraphSchema schema,
                       TreeNodeLabelManager labelManager,
                       Configuration configuration) {
        this.treeLeaf = treeLeaf;
        this.schema = schema;
        this.labelManager = labelManager;
        this.configuration = configuration;

        validExecuteMode();
    }

    private void validExecuteMode() {
        ExecuteMode executeMode = ExecuteMode.valueOf(StringUtils.upperCase(
                configuration.getString(
                        CompilerConstant.QUERY_EXECUTE_MODE,
                        ExecuteMode.AUTO.name())));
        if (executeMode == ExecuteMode.TINKERPOP) {
            throw new IllegalArgumentException("Use tinkerpop to execute this query for execute mode is " + executeMode.toString());
        }
    }

    public TreeNode getSourceNode() {
        return CompilerUtils.getSourceTreeNode(treeLeaf);
    }

    public TreeNodeLabelManager getLabelManager() {
        return labelManager;
    }

    public void applyStrategy() {
        for (GraphTreeStrategy graphTreeStrategy : graphTreeStrategyList) {
            graphTreeStrategy.apply(this);
        }
    }

    public void optimizeTree() {
        applyStrategy();
        validOrderResult();
        optimizeOrderRange();
        optimizeLimitCount();
        optimizeEdgePropFlag();
    }

    public CostModelManager optimizeCostModel() {
        List<TreeNode> treeNodeList = TreeNodeUtils.buildTreeNodeListFromLeaf(this.getTreeLeaf());
        CostGraph costGraph = CostUtils.buildCostGraph(this.getTreeLeaf(), this.labelManager);
        NodeLabelManager nodeLabelManager = new NodeLabelManager();
        NodeLabelList previousNodeLabel = null;
        for (TreeNode treeNode : treeNodeList) {
            NodeLabelList nodeLabelList = NodeLabelList.buildNodeLabel(previousNodeLabel, treeNode, schema);
            nodeLabelManager.addNodeLabelList(nodeLabelList);
            previousNodeLabel = nodeLabelList;
        }
        int pathIndex = this.getQueryConfig().getInt(CompilerConstant.QUERY_COSTMODEL_PLAN_PATH, -1);
        List<CostPath> costPathList = costGraph.getCostPathList();
        CostPath useCostPath;
        if (pathIndex < 0 || pathIndex >= costPathList.size()) {
            CostDataStatistics costDataStatistics = CostDataStatistics.getInstance();
            List<Double> stepCountList = costDataStatistics.computeStepCountList(nodeLabelManager, treeNodeList);
            List<Double> shuffleThresholdList = Lists.newArrayList(1.0);
            for (int i = 1; i < stepCountList.size(); i++) {
                shuffleThresholdList.add(1.5);
            }
            useCostPath = costGraph.computePath(stepCountList, shuffleThresholdList);
        } else {
            useCostPath = costPathList.get(pathIndex);
            logger.info("Use specify cost path " + useCostPath.toString());
        }
        return new CostModelManager(costGraph, useCostPath);
    }

    /**
     * Optimize to open prop flag for edge
     */
    private void optimizeEdgePropFlag() {
        TreeNode currentTreeNode = treeLeaf;
        while (!(currentTreeNode instanceof SourceTreeNode)) {
            if (currentTreeNode instanceof EdgeTreeNode) {
                EdgeTreeNode edgeTreeNode = EdgeTreeNode.class.cast(currentTreeNode);
                TreeNode nextTreeNode = edgeTreeNode.getOutputNode();
                if (null != nextTreeNode &&
                        edgeTreeNode.beforeRequirementList.isEmpty() &&
                        edgeTreeNode.afterRequirementList.isEmpty()) {
                    if (nextTreeNode instanceof EdgeVertexTreeNode ||
                            (nextTreeNode.getNodeType() == NodeType.AGGREGATE &&
                                    !(nextTreeNode instanceof GroupTreeNode))) {
                        edgeTreeNode.setFetchPropFlag(true);
                    }

                }
            }
            currentTreeNode = UnaryTreeNode.class.cast(currentTreeNode).getInputNode();
        }
    }

    /**
     * Optimize order+range operator
     */
    private void optimizeOrderRange() {
        TreeNode currentTreeNode = treeLeaf;
        while (!(currentTreeNode instanceof SourceTreeNode)) {
            if (currentTreeNode instanceof OrderGlobalTreeNode) {
                OrderGlobalTreeNode orderGlobalTreeNode = OrderGlobalTreeNode.class.cast(currentTreeNode);
                TreeNode orderInputNode = orderGlobalTreeNode.getInputNode();
                if (orderInputNode instanceof SourceVertexTreeNode
                        && ((SourceVertexTreeNode) orderInputNode).getBeforeRequirementList().isEmpty()
                        && ((SourceVertexTreeNode) orderInputNode).getAfterRequirementList().isEmpty()
                        && orderGlobalTreeNode.isEmptyOrderNode()
                        && !orderGlobalTreeNode.orderFlag
                        && null != orderGlobalTreeNode.rangeLimit) {
                    RangeLimit.Builder orderRangeBuilder = orderGlobalTreeNode.rangeLimit;
                    orderInputNode.setRangeLimit(0, orderRangeBuilder.getRangeEnd(), true);
                    ((SourceVertexTreeNode) orderInputNode).enablePartitionIdFlag();
                    orderGlobalTreeNode.enablePartitionIdFlag();
                }
            }
            currentTreeNode = UnaryTreeNode.class.cast(currentTreeNode).getInputNode();
        }
    }

    /**
     * Add order node to reorder result
     */
    private void validOrderResult() {
        TreeNode currentTreeNode = treeLeaf;
        TreeNode orderTreeNode = null;
        while (!(currentTreeNode instanceof SourceTreeNode)) {
            if (currentTreeNode instanceof OrderGlobalTreeNode) {
                orderTreeNode = currentTreeNode;
                break;
            } else {
                currentTreeNode = UnaryTreeNode.class.cast(currentTreeNode).getInputNode();
            }
        }
        if (null != orderTreeNode) {
            OrderGlobalTreeNode orderGlobalTreeNode = OrderGlobalTreeNode.class.cast(orderTreeNode);
            TreeNode aggTreeNode = orderTreeNode.getOutputNode();
            while (aggTreeNode != null && aggTreeNode.getNodeType() != NodeType.AGGREGATE) {
                aggTreeNode = aggTreeNode.getOutputNode();
            }
            if (null != aggTreeNode) {
                if (aggTreeNode instanceof FoldTreeNode) {
                    TreeNode inputTreeNode = UnaryTreeNode.class.cast(aggTreeNode).getInputNode();
                    if (inputTreeNode == orderTreeNode) {
                        return;
                    }
                    UnaryTreeNode inputUnaryTreeNode = UnaryTreeNode.class.cast(inputTreeNode);
                    if (inputUnaryTreeNode.getInputNode() == orderTreeNode &&
                            (inputUnaryTreeNode instanceof EdgeVertexTreeNode &&
                                    EdgeVertexTreeNode.class.cast(inputUnaryTreeNode).getDirection() != Direction.BOTH)) {
                        return;
                    }
                    String orderLabel = orderGlobalTreeNode.enableOrderFlag(labelManager);
                    SelectOneTreeNode selectOneTreeNode = new SelectOneTreeNode(new SourceDelegateNode(inputUnaryTreeNode, schema), orderLabel, Pop.last, Lists.newArrayList(), schema);
                    selectOneTreeNode.setConstantValueType(new ValueValueType(VariantType.VT_INTEGER));
                    OrderGlobalTreeNode addOrderTreeNode = new OrderGlobalTreeNode(inputUnaryTreeNode, schema,
                            Lists.newArrayList(Pair.of(selectOneTreeNode, Order.incr)));
                    UnaryTreeNode.class.cast(aggTreeNode).setInputNode(addOrderTreeNode);
                }
            } else {
                if (treeLeaf instanceof OrderGlobalTreeNode) {
                    return;
                }
                TreeNode currTreeNode = orderTreeNode.getOutputNode();
                boolean hasSimpleShuffle = false;
                while (currTreeNode != null) {
                    if (currTreeNode.getNodeType() == NodeType.FLATMAP
                            || (currTreeNode instanceof PropertyNode &&
                            !(UnaryTreeNode.class.cast(currTreeNode).getInputNode().getOutputValueType() instanceof EdgeValueType))) {
                        hasSimpleShuffle = true;
                        break;
                    }
                    currTreeNode = currTreeNode.getOutputNode();
                }
                if (!hasSimpleShuffle) {
                    return;
                }

                UnaryTreeNode outputTreeNode = UnaryTreeNode.class.cast(treeLeaf);
                if (outputTreeNode.getInputNode() == orderTreeNode) {
                    if (outputTreeNode instanceof EdgeVertexTreeNode &&
                            EdgeVertexTreeNode.class.cast(outputTreeNode).getDirection() != Direction.BOTH) {
                        return;
                    }
                    if (orderTreeNode.getOutputValueType() instanceof EdgeValueType && outputTreeNode instanceof PropertyNode) {
                        return;
                    }
                }
                String orderLabel = orderGlobalTreeNode.enableOrderFlag(labelManager);
                SelectOneTreeNode selectOneTreeNode = new SelectOneTreeNode(new SourceDelegateNode(treeLeaf, schema), orderLabel, Pop.last, Lists.newArrayList(), schema);
                selectOneTreeNode.setConstantValueType(new ValueValueType(VariantType.VT_INTEGER));
                treeLeaf = new OrderGlobalTreeNode(treeLeaf, schema,
                        Lists.newArrayList(Pair.of(selectOneTreeNode, Order.incr)));
            }
        }
    }

    /**
     * Optimize limit->count to LimitCount Operator
     */
    private void optimizeLimitCount() {
        TreeNode currentTreeNode = treeLeaf;
        while (!(currentTreeNode instanceof SourceTreeNode)) {
            if (currentTreeNode instanceof CountGlobalTreeNode) {
                CountGlobalTreeNode countGlobalTreeNode = CountGlobalTreeNode.class.cast(currentTreeNode);
                if (countGlobalTreeNode.getAfterRequirementList().isEmpty() &&
                        countGlobalTreeNode.getBeforeRequirementList().isEmpty() &&
                        countGlobalTreeNode.getInputNode() instanceof RangeGlobalTreeNode) {
                    RangeGlobalTreeNode rangeGlobalTreeNode = RangeGlobalTreeNode.class.cast(countGlobalTreeNode.getInputNode());
                    if (rangeGlobalTreeNode.getAfterRequirementList().isEmpty() &&
                            rangeGlobalTreeNode.getBeforeRequirementList().isEmpty() &&
                            rangeGlobalTreeNode.getLow() == 0 &&
                            rangeGlobalTreeNode.getHigh() > 0) {
                        countGlobalTreeNode.setLimitCount(rangeGlobalTreeNode.getHigh());
                        countGlobalTreeNode.setInputNode(rangeGlobalTreeNode.getInputNode());
                        if (countGlobalTreeNode.getInputNode() instanceof EdgeTreeNode) {
                            ((EdgeTreeNode) countGlobalTreeNode.getInputNode()).setFetchPropFlag(true);
                        }
                    }
                }
            }
            currentTreeNode = UnaryTreeNode.class.cast(currentTreeNode).getInputNode();
        }
    }

    public TreeNode getTreeLeaf() {
        return treeLeaf;
    }

    public void setLeafNode(TreeNode leafNode) {
        this.treeLeaf = leafNode;
    }

    public Configuration getQueryConfig() {
        return this.configuration;
    }

    public GraphSchema getSchema() {
        return this.schema;
    }
}
