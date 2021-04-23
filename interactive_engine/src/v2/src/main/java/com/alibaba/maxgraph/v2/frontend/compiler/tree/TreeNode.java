package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.RequirementValue;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.List;
import java.util.Set;

public interface TreeNode {

    /**
     * Get the type of given tree node
     *
     * @return The node type
     */
    NodeType getNodeType();

    /**
     * Get output node of this tree node
     *
     * @return The output node
     */
    TreeNode getOutputNode();

    /**
     * Set the output node of this tree node
     *
     * @param treeNode The output node
     */
    void setOutputNode(TreeNode treeNode);

    /**
     * Build sub logical query plan for the given tree node
     *
     * @return The result logical query plan
     */
    LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager);

    /**
     * Set output vertex for this tree node
     *
     * @param finishVertex The output vertex
     */
    void setFinishVertex(LogicalVertex finishVertex, TreeNodeLabelManager treeNodeLabelManager);

    /**
     * Get the output vertex of this tree node
     *
     * @return The output vertex
     */
    LogicalVertex getOutputVertex();

    /**
     * Add label start for this tree node
     *
     * @param label The start label
     */
    void addLabelStartRequirement(String label);

    /**
     * Add path requirement for this tree node
     */
    void addPathRequirement();

    /**
     * Build after requirement value list
     */
    List<RequirementValue.Builder> buildAfterRequirementList(TreeNodeLabelManager nodeLabelManager);

    /**
     * Build before requirement value list
     */
    List<RequirementValue.Builder> buildBeforeRequirementList(TreeNodeLabelManager nodeLabelManager);

    /**
     * Get used label list
     */
    Set<String> getUsedLabelList();

    /**
     * The output value type of this tree node
     *
     * @return The output value type
     */
    ValueType getOutputValueType();

    /**
     * Check if the output of this node can be added to path
     *
     * @return The path flag
     */
    boolean isPathFlag();

    /**
     * Set local range limit for this operator
     *
     * @param low             the low
     * @param high            the high
     * @param globalRangeFlag If true, the range is global, else the range is by key
     */
    void setRangeLimit(long low, long high, boolean globalRangeFlag);

    /**
     * Add has container list to tree node
     *
     * @param hasContainerList The given has container list
     */
    void addHasContainerList(List<HasContainer> hasContainerList);

    /**
     * Check if the result of this node can access edge/property locally
     *
     * @return The prop local flag
     */
    boolean isPropLocalFlag();

    /**
     * Open dedup local optimize
     */
    void enableDedupLocal();

    /**
     * If this operator open dedup local optimize
     *
     * @return The dedup local flag
     */
    boolean isDedupLocal();

    /**
     * Set this node as subquery node
     */
    void setSubqueryNode();

    /**
     * Check if this node is subquery node
     *
     * @return The subquery node flag
     */
    boolean isSubqueryNode();

    /**
     * Enable global stop by global limit operator
     */
    void enableGlobalStop();

    /**
     * Enable global filter by global stop
     */
    void enableGlobalFilter();
}
