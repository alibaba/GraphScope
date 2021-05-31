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
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.PropertyNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.EdgeValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.VarietyValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.CompilerUtils;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.SchemaUtils;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.TreeNodeUtils;
import com.google.common.collect.Sets;

import java.util.Set;

public class ElementValueTreeNode extends UnaryTreeNode implements PropertyNode {
    private String propKey;
    private TreeNode bypassTreeNode;

    public ElementValueTreeNode(TreeNode input, String propKey, TreeNode bypassTreeNode, GraphSchema schema) {
        super(input, NodeType.MAP, schema);
        this.propKey = propKey;
        this.bypassTreeNode = bypassTreeNode;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        if (null == bypassTreeNode) {
            Value.Builder argumentBuilder = Value.newBuilder()
                    .addIntValueList(SchemaUtils.getPropId(propKey, schema))
                    .setBoolFlag(edgePropFlag());
            ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.PROP_VALUE, argumentBuilder);

            return parseSingleUnaryVertex(contextManager.getVertexIdManager(),
                    contextManager.getTreeNodeLabelManager(),
                    processorFunction,
                    contextManager);
        } else {
            LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
            LogicalVertex delegateSourceVertex = getInputNode().getOutputVertex();
            logicalSubQueryPlan.addLogicalVertex(delegateSourceVertex);

            LogicalSubQueryPlan bypassPlan = TreeNodeUtils.buildQueryPlanWithSource(
                    bypassTreeNode,
                    contextManager.getTreeNodeLabelManager(),
                    contextManager,
                    contextManager.getVertexIdManager(),
                    delegateSourceVertex);
            logicalSubQueryPlan.mergeLogicalQueryPlan(bypassPlan);
            LogicalVertex outputVertex = bypassPlan.getOutputVertex();
            setFinishVertex(outputVertex, contextManager.getTreeNodeLabelManager());
            addUsedLabelAndRequirement(outputVertex, contextManager.getTreeNodeLabelManager());

            return logicalSubQueryPlan;
        }
    }

    @Override
    public ValueType getOutputValueType() {
        if (null == bypassTreeNode) {
            Set<DataType> dataTypeSet = SchemaUtils.getDataTypeList(propKey, schema);
            if (dataTypeSet.size() > 1) {
                Set<ValueType> valueTypeList = Sets.newHashSet();
                for (DataType dataType : dataTypeSet) {
                    valueTypeList.add(new ValueValueType(CompilerUtils.parseVariantFromDataType(dataType)));
                }
                return new VarietyValueType(valueTypeList);
            } else {
                return new ValueValueType(CompilerUtils.parseVariantFromDataType(dataTypeSet.iterator().next()));
            }
        }
        return bypassTreeNode.getOutputValueType();
    }

    @Override
    public Set<String> getPropKeyList() {
        if (null == bypassTreeNode) {
            return Sets.newHashSet(propKey);
        }

        return Sets.newHashSet();
    }

    @Override
    public boolean isPropLocalFlag() {
        if (null == bypassTreeNode) {
            return true;
        } else {
            return bypassTreeNode.isPropLocalFlag();
        }
    }

    @Override
    public boolean edgePropFlag() {
        return getInputNode().getOutputValueType() instanceof EdgeValueType;
    }

    public TreeNode getByPassTraversal() {
        return bypassTreeNode;
    }
}
