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
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.source.SourceVertexTreeNode;
import com.alibaba.maxgraph.common.util.SchemaUtils;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.tree.addition.PropertyNode;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Read the given property values for vertex
 */
public class PropFillTreeNode extends UnaryTreeNode implements PropertyNode {
    private Set<String> propKeyList;

    public PropFillTreeNode(TreeNode input, Set<String> propKeyList, GraphSchema schema) {
        super(input, NodeType.MAP, schema);
        this.propKeyList = Sets.newHashSet();
        if (null != propKeyList) {
            this.propKeyList.addAll(propKeyList);
        }
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        Message.Value.Builder argumentBuilder = Message.Value.newBuilder();
        propKeyList.forEach(v -> argumentBuilder.addIntValueList(SchemaUtils.getPropId(v, schema)));
        ProcessorFunction processorFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.PROP_FILL, argumentBuilder);

        if (getInputNode() instanceof SourceVertexTreeNode) {
            return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager, new LogicalEdge(EdgeShuffleType.FORWARD));
        } else {
            return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
        }
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }

    @Override
    public Set<String> getPropKeyList() {
        return propKeyList;
    }

    @Override
    public boolean isPropLocalFlag() {
        return true;
    }

    @Override
    public boolean edgePropFlag() {
        return false;
    }
}
