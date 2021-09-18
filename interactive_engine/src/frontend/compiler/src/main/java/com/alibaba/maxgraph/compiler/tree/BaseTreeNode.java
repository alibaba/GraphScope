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
package com.alibaba.maxgraph.compiler.tree;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.QueryFlowOuterClass.RequirementType;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.tree.source.SourceVertexTreeNode;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.maxgraph.QueryFlowOuterClass.RequirementType.*;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class BaseTreeNode implements TreeNode {
    private static final Logger logger = LoggerFactory.getLogger(BaseTreeNode.class);
    private TreeNode output = null;
    protected NodeType nodeType;
    protected QueryFlowOuterClass.RangeLimit.Builder rangeLimit = null;
    protected LogicalVertex outputVertex;
    protected List<HasContainer> hasContainerList = Lists.newArrayList();

    boolean globalRangeFlag = true;
    Map<RequirementType, Object> afterRequirementList = Maps.newHashMap();
    Map<RequirementType, Object> beforeRequirementList = Maps.newHashMap();

    /**
     * Early stop related argument
     */
    protected QueryFlowOuterClass.EarlyStopArgument.Builder earlyStopArgument
            = QueryFlowOuterClass.EarlyStopArgument.newBuilder();

    /**
     * If true, the output of this node can be added to path
     */
    private boolean pathFlag;
    /**
     * If ture, it can access edge and property locally after this operator executed
     */
    private boolean propLocalFlag;

    /**
     * Label list is used in this tree node
     */
    private Set<String> usedLabelList = Sets.newHashSet();
    /**
     * If this operator open dedup local optimize
     */
    private boolean dedupLocalFlag = false;
    /**
     * Describe if this node is a subquery node
     */
    private boolean subqueryNodeFlag = false;

    protected GraphSchema schema;

    public BaseTreeNode(NodeType nodeType, GraphSchema schema) {
        this.nodeType = nodeType;
        this.schema = schema;
        this.outputVertex = null;
        this.pathFlag = false;
        this.propLocalFlag = false;
    }

    @Override
    public TreeNode getOutputNode() {
        return output;
    }

    @Override
    public void setOutputNode(TreeNode output) {
        this.output = output;
    }

    @Override
    public NodeType getNodeType() {
        return this.nodeType;
    }

    @Override
    public void setFinishVertex(LogicalVertex vertex, TreeNodeLabelManager treeNodeLabelManager) {
        this.outputVertex = vertex;
        if (null != treeNodeLabelManager) {
            hasContainerList.forEach(v -> {
                Message.LogicalCompare logicalCompare = CompilerUtils.parseLogicalCompare(v, schema, treeNodeLabelManager.getLabelIndexList(), this instanceof SourceVertexTreeNode);
                vertex.getProcessorFunction().getLogicalCompareList().add(logicalCompare);
            });
        }
    }

    @Override
    public LogicalVertex getOutputVertex() {
        return checkNotNull(outputVertex);
    }

    @Override
    public void addLabelStartRequirement(String label) {
        Object labelList = this.afterRequirementList.get(LABEL_START);
        if (null != labelList) {
            ((Set<String>) labelList).add(checkNotNull(label));
        } else {
            this.afterRequirementList.put(LABEL_START, Sets.newHashSet(label));
        }
    }

    @Override
    public List<QueryFlowOuterClass.RequirementValue.Builder> buildAfterRequirementList(TreeNodeLabelManager nodeLabelManager) {
        return getRequirementList(afterRequirementList, Arrays.asList(LABEL_START), nodeLabelManager);
    }

    @Override
    public List<QueryFlowOuterClass.RequirementValue.Builder> buildBeforeRequirementList(TreeNodeLabelManager nodeLabelManager) {
        return getRequirementList(beforeRequirementList, Arrays.asList(PATH_ADD, LABEL_START), nodeLabelManager);
    }

    private List<QueryFlowOuterClass.RequirementValue.Builder> getRequirementList(Map<RequirementType, Object> requirementMap,
                                                                                  List<RequirementType> requirementTypes,
                                                                                  TreeNodeLabelManager nodeLabelManager) {
        List<QueryFlowOuterClass.RequirementValue.Builder> requirementList = Lists.newArrayList();
        for (RequirementType type : requirementTypes) {
            if (!requirementMap.containsKey(type)) {
                logger.warn("requirement type {} not exist in map", type);
                continue;
            }
            Object conf = requirementMap.get(type);
            switch (type) {
                case LABEL_START: {
                    Set<String> startLabelList = (Set<String>) conf;
                    if (startLabelList == null || startLabelList.isEmpty()) {
                        throw new IllegalArgumentException("There's label start requirement but start label list is empty");
                    }
                    Message.Value.Builder valueBuilder = Message.Value.newBuilder();
                    for (String startLabel : startLabelList) {
                        valueBuilder.addIntValueList(nodeLabelManager.getLabelIndex(startLabel));
                    }
                    requirementList.add(QueryFlowOuterClass.RequirementValue.newBuilder()
                            .setReqArgument(valueBuilder)
                            .setReqType(type));
                    break;
                }
                case PATH_ADD: {
                    Message.Value.Builder valueBuilder = Message.Value.newBuilder();
                    requirementList.add(QueryFlowOuterClass.RequirementValue.newBuilder()
                            .setReqArgument(valueBuilder)
                            .setReqType(type));
                    break;
                }
                default: {
                    throw new IllegalArgumentException(type + " can't exist in before requirement");
                }
            }
        }
        return requirementList;
    }

    @Override
    public Set<String> getUsedLabelList() {
        return usedLabelList;
    }

    @Override
    public void addPathRequirement() {
        this.beforeRequirementList.put(PATH_ADD, null);
    }

    @Override
    public boolean isPathFlag() {
        return pathFlag;
    }

    public void setPathFlag(boolean pathFlag) {
        this.pathFlag = pathFlag;
    }

    @Override
    public boolean isPropLocalFlag() {
        return propLocalFlag;
    }

    public void setPropLocalFlag(boolean propLocalFlag) {
        this.propLocalFlag = propLocalFlag;
    }

    @Override
    public void setRangeLimit(long low, long high, boolean globalRangeFlag) {
        this.rangeLimit = QueryFlowOuterClass.RangeLimit.newBuilder()
                .setRangeStart(low)
                .setRangeEnd(high);
        this.globalRangeFlag = globalRangeFlag;
    }

    public Map<RequirementType, Object> getAfterRequirementList() {
        return afterRequirementList;
    }

    public Map<RequirementType, Object> getBeforeRequirementList() {
        return beforeRequirementList;
    }

    public void addHasContainer(HasContainer hasContainer) {
        hasContainerList.add(hasContainer);
    }

    public List<HasContainer> getHasContainerList() {
        return hasContainerList;
    }

    @Override
    public void addHasContainerList(List<HasContainer> hasContainerList) {
        hasContainerList.forEach(v -> addHasContainer(v));
    }

    @Override
    public void enableDedupLocal() {
        this.dedupLocalFlag = true;
    }

    @Override
    public boolean isDedupLocal() {
        return this.dedupLocalFlag;
    }

    /**
     * Set this node as subquery node
     */
    public void setSubqueryNode() {
        this.subqueryNodeFlag = true;
    }

    /**
     * Check if this node is subquery node
     *
     * @return The subquery node flag
     */
    public boolean isSubqueryNode() {
        return this.subqueryNodeFlag;
    }

    protected Message.Value.Builder createArgumentBuilder() {
        return Message.Value.newBuilder()
                .setDedupLocalFlag(dedupLocalFlag)
                .setSubqueryFlag(subqueryNodeFlag);
    }


    /**
     * Enable global stop by global limit operator
     */
    @Override
    public void enableGlobalStop() {
        this.earlyStopArgument.setGlobalStopFlag(true);
    }

    /**
     * Enable global filter by global stop
     */
    @Override
    public void enableGlobalFilter() {
        this.earlyStopArgument.setGlobalFilterFlag(true);
    }

    public boolean removeBeforeLabel(String label) {
        Set<String> labelList = (Set<String>) this.beforeRequirementList.get(LABEL_START);
        if (null != labelList && labelList.contains(label)) {
            labelList.remove(label);
            if (labelList.isEmpty()) {
                this.beforeRequirementList.remove(LABEL_START);
            }
            return true;
        }
        return false;
    }
}
