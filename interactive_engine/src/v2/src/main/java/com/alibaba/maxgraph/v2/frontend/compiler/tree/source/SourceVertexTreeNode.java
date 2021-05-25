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
package com.alibaba.maxgraph.v2.frontend.compiler.tree.source;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSourceVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorSourceFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeNodeLabelManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.CountFlagNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.SchemaUtils;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.ListUtils;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SourceVertexTreeNode extends SourceTreeNode implements CountFlagNode {
    private static final Logger logger = LoggerFactory.getLogger(SourceVertexTreeNode.class);
    private boolean countFlag = false;
    /**
     * Use worker index as flow message'id
     */
    private boolean partitionIdFlag = false;

    public SourceVertexTreeNode(GraphSchema schema) {
        this(null, schema);
    }

    public SourceVertexTreeNode(Object[] ids, GraphSchema schema) {
        super(ids, schema);
        setPropLocalFlag(true);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalQueryPlan = new LogicalSubQueryPlan(contextManager);
        logicalQueryPlan.setDelegateSourceFlag(false);

        Value.Builder argumentBuilder = Value.newBuilder();
        boolean labelFlag = processLabelArgument(argumentBuilder, true);
        boolean idFlag = processIdArgument(argumentBuilder, true);
        if (!idFlag && labelFlag && argumentBuilder.getIntValueListCount() > 0) {
            List<Integer> labelIdList = argumentBuilder.getIntValueListList();
            Set<Integer> primaryKeySet = Sets.newHashSet();
            Map<Integer, HasContainer> primaryKeyContainerList = Maps.newHashMap();
            if (labelIdList.size() == 1) {
                int vertexLabelId = labelIdList.get(0);
                VertexType vertexType = (VertexType) schema.getSchemaElement(vertexLabelId);
                List<Integer> primaryKeyIdList = SchemaUtils.getVertexPrimaryKeyList(vertexType);
                primaryKeySet.addAll(primaryKeyIdList);
                processPrimaryKey(argumentBuilder, vertexLabelId, primaryKeyIdList, primaryKeyContainerList);

                if (primaryKeyContainerList.keySet().containsAll(primaryKeySet)) {
                    for (int primaryKey : primaryKeySet) {
                        HasContainer hasContainer = primaryKeyContainerList.get(primaryKey);
                        if (null != hasContainer) {
                            this.hasContainerList.remove(hasContainer);
                        }
                    }
                }
            } else {
                Map<Integer, List<Integer>> labelPrimaryKeyList = Maps.newHashMap();
                boolean valid = true;
                for (int vertexLabelId : labelIdList) {
                    try {
                        VertexType vertexType = (VertexType) schema.getSchemaElement(vertexLabelId);
                        List<Integer> currPrimaryKeyIdList = SchemaUtils.getVertexPrimaryKeyList(vertexType);
                        primaryKeySet.addAll(currPrimaryKeyIdList);
                        for (List<Integer> primaryKeyIdList : labelPrimaryKeyList.values()) {
                            if (!ListUtils.isEqualList(primaryKeyIdList, currPrimaryKeyIdList)) {
                                valid = false;
                                break;
                            }
                        }
                        if (!valid) {
                            break;
                        } else {
                            labelPrimaryKeyList.put(vertexLabelId, currPrimaryKeyIdList);
                        }
                    } catch (Exception e) {
                        logger.error("parse vertex type fail", e);
                    }
                }
                if (valid) {
                    for (Map.Entry<Integer, List<Integer>> entry : labelPrimaryKeyList.entrySet()) {
                        processPrimaryKey(argumentBuilder, entry.getKey(), entry.getValue(), primaryKeyContainerList);
                    }

                    if (primaryKeyContainerList.keySet().containsAll(primaryKeySet)) {
                        for (int primaryKey : primaryKeySet) {
                            HasContainer hasContainer = primaryKeyContainerList.get(primaryKey);
                            if (null != hasContainer) {
                                this.hasContainerList.remove(hasContainer);
                            }
                        }
                    }
                }
            }
        }
        argumentBuilder.setBoolFlag(isPartitionIdFlag());

        ProcessorSourceFunction processorSourceFunction = new ProcessorSourceFunction(getCountOperator(OperatorType.V), argumentBuilder, rangeLimit);
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        TreeNodeLabelManager treeNodeLabelManager = contextManager.getTreeNodeLabelManager();
        LogicalSourceVertex logicalSourceVertex = new LogicalSourceVertex(vertexIdManager.getId(), processorSourceFunction);

        logicalSourceVertex.getBeforeRequirementList().addAll(buildBeforeRequirementList(treeNodeLabelManager));
        logicalSourceVertex.getAfterRequirementList().addAll(buildAfterRequirementList(treeNodeLabelManager));
        getUsedLabelList().forEach(v -> processorSourceFunction.getUsedLabelList().add(treeNodeLabelManager.getLabelIndex(v)));
        logicalQueryPlan.addLogicalVertex(logicalSourceVertex);

        setFinishVertex(logicalQueryPlan.getOutputVertex(), treeNodeLabelManager);

        return logicalQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return getCountOutputType(new VertexValueType());
    }


    @Override
    public boolean checkCountOptimize() {
        return false;
    }

    @Override
    public void enableCountFlag() {
        this.countFlag = true;
    }

    @Override
    public boolean isCountFlag() {
        return countFlag;
    }

    public boolean isPartitionIdFlag() {
        return partitionIdFlag;
    }

    public void enablePartitionIdFlag() {
        this.partitionIdFlag = true;
    }
}
