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
package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.PathOutType;
import com.alibaba.maxgraph.proto.v2.PathOutValue;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.PropertyNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.source.SourceDelegateNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.PathValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.SchemaUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PathTreeNode extends UnaryTreeNode {
    private List<TreeNode> ringTreeNodeList;
    private Set<ValueType> pathValueList;
    private boolean pathDeleteFlag = true;

    public PathTreeNode(TreeNode input, GraphSchema schema) {
        super(input, NodeType.MAP, schema);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        Value.Builder argumentBuilder = Value.newBuilder()
                .addAllPathOutValue(getPathOutValueList(labelManager.getLabelIndexList()))
                .setBoolValue(pathDeleteFlag);
        ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.PATH_OUT, argumentBuilder);

        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager, new LogicalEdge(EdgeShuffleType.FORWARD));
    }

    private List<PathOutValue> getPathOutValueList(Map<String, Integer> labelIndexList) {
        List<PathOutValue> pathOutValueList = Lists.newArrayList();
        if (null == ringTreeNodeList || ringTreeNodeList.isEmpty()) {
            return pathOutValueList;
        }
        for (TreeNode treeNode : ringTreeNodeList) {
            if (treeNode instanceof SourceDelegateNode) {
                pathOutValueList.add(PathOutValue.newBuilder()
                        .setPathOutType(PathOutType.PATH_VALUE).build());
            } else {
                UnaryTreeNode unaryTreeNode = UnaryTreeNode.class.cast(treeNode);
                if (unaryTreeNode.getInputNode() instanceof SourceDelegateNode) {
                    if (unaryTreeNode instanceof ElementValueTreeNode) {
                        String propKey = Lists.newArrayList(ElementValueTreeNode.class.cast(unaryTreeNode).getPropKeyList()).get(0);
                        pathOutValueList.add(PathOutValue.newBuilder()
                                .setPathOutType(PathOutType.PATH_PROP)
                                .setPathPropId(SchemaUtils.getPropId(propKey, schema)).build());
                    } else if (unaryTreeNode instanceof TokenTreeNode) {
                        pathOutValueList.add(PathOutValue.newBuilder()
                                .setPathOutType(PathOutType.PATH_PROP)
                                .setPathPropId(labelIndexList.get(TokenTreeNode.class.cast(unaryTreeNode).getToken().getAccessor())).build());
                    } else {
                        throw new IllegalArgumentException("Only support path().by().by(T.id/label).by(\"propName\") yet");
                    }
                } else {
                    throw new IllegalArgumentException("Only support path().by().by(T.id/label).by(\"propName\") yet");
                }
            }
        }

        return pathOutValueList;
    }

    @Override
    public ValueType getOutputValueType() {
        PathValueType pathValueType = new PathValueType();
        if (null == ringTreeNodeList || ringTreeNodeList.isEmpty()) {
            for (ValueType valueType : pathValueList) {
                pathValueType.addPathValueType(valueType);
            }
        } else {
            for (TreeNode treeNode : ringTreeNodeList) {
                pathValueType.addPathValueType(treeNode.getOutputValueType());
            }
        }
        return pathValueType;
    }

    public void setRingTreeNodeList(List<TreeNode> ringTreeNodeList) {
        this.ringTreeNodeList = ringTreeNodeList;
    }

    public Set<String> getOutputPropList() {
        Set<String> outputPropList = Sets.newHashSet();
        for (TreeNode treeNode : ringTreeNodeList) {
            if (treeNode instanceof PropertyNode) {
                outputPropList.addAll(PropertyNode.class.cast(treeNode).getPropKeyList());
            }
        }
        return outputPropList;
    }

    @Override
    public boolean isPropLocalFlag() {
        return true;
    }

    public void disablePathDelete() {
        this.pathDeleteFlag = false;
    }

    public void setPathValueList(Set<ValueType> pathValueList) {
        this.pathValueList = pathValueList;
    }
}
