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
package com.alibaba.maxgraph.compiler.tree.source;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalSourceVertex;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.TreeNodeLabelManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorSourceFunction;
import com.alibaba.maxgraph.compiler.tree.addition.CountFlagNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

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

        Message.Value.Builder argumentBuilder = Message.Value.newBuilder();
        processLabelArgument(argumentBuilder, true);
        processIdArgument(argumentBuilder, true);

        Map<String, List<Integer>> primaryKeyLabelIdList = Maps.newHashMap();
        for (GraphVertex vertexType : this.schema.getVertexList()) {
            List<GraphProperty> primaryKeyList = vertexType.getPrimaryKeyList();
            if (null != primaryKeyList && primaryKeyList.size() == 1) {
                String propertyName = primaryKeyList.get(0).getName();
                List<Integer> labelIdList = primaryKeyLabelIdList.computeIfAbsent(propertyName, k -> Lists.newArrayList());
                labelIdList.add(vertexType.getLabelId());
            }
        }

        QueryFlowOuterClass.VertexPrimaryKeyListProto.Builder primaryKeyBuilder = QueryFlowOuterClass.VertexPrimaryKeyListProto.newBuilder();
        for (HasContainer container : this.hasContainerList) {
            String key = container.getKey();
            List<Integer> labelIdList = primaryKeyLabelIdList.get(key);
            if (null != labelIdList) {
                for (Integer labelId : labelIdList) {
                    if (container.getBiPredicate() instanceof Compare
                            && container.getBiPredicate() == Compare.eq) {
                        primaryKeyBuilder.addPrimaryKeys(QueryFlowOuterClass.VertexPrimaryKeyProto.newBuilder()
                                .setLabelId(labelId)
                                .setPrimaryKeyValue(container.getValue().toString()));
                    } else if (container.getBiPredicate() instanceof Contains
                            && container.getBiPredicate() == Contains.within) {
                        for (Object val : (Collection<Object>) container.getValue()) {
                            primaryKeyBuilder.addPrimaryKeys(QueryFlowOuterClass.VertexPrimaryKeyProto.newBuilder()
                                    .setLabelId(labelId)
                                    .setPrimaryKeyValue(val.toString()));
                        }
                    }
                }
            }
        }
        argumentBuilder.setPayload(primaryKeyBuilder.build().toByteString())
                .setBoolFlag(isPartitionIdFlag());

        ProcessorSourceFunction processorSourceFunction = new ProcessorSourceFunction(getCountOperator(QueryFlowOuterClass.OperatorType.V), argumentBuilder, rangeLimit);
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
