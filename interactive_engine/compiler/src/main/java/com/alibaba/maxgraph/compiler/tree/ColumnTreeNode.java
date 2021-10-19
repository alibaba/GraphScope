/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.tree;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.ListValueType;
import com.alibaba.maxgraph.compiler.tree.value.MapEntryValueType;
import com.alibaba.maxgraph.compiler.tree.value.MapValueType;
import com.alibaba.maxgraph.compiler.tree.value.PathValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Column;

import java.util.List;
import java.util.Map;

/**
 * Output keys or values 1. keys a> for map, output the keys list b> for Map.Entry, output the key
 * c> for Path, output the labels list, List<Set<String>> 2. values a> for map, output the values
 * list b> for Map.Entry, output the value c> for Path, output the value list without label
 */
public class ColumnTreeNode extends UnaryTreeNode {
    private Column column;

    public ColumnTreeNode(TreeNode input, GraphSchema schema, Column column) {
        super(input, NodeType.MAP, schema);
        this.column = column;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        Map<String, Integer> labelIndexList =
                contextManager.getTreeNodeLabelManager().getLabelIndexList();
        List<Integer> labelIdList = Lists.newArrayList();
        List<String> labelNameList = Lists.newArrayList();
        for (Map.Entry<String, Integer> entry : labelIndexList.entrySet()) {
            if (entry.getValue() > TreeConstants.SYS_LABEL_START
                    && entry.getValue() <= TreeConstants.USER_LABEL_START)
                labelIdList.add(entry.getValue());
            labelNameList.add(entry.getKey());
        }

        ProcessorFunction processorFunction =
                new ProcessorFunction(
                        QueryFlowOuterClass.OperatorType.COLUMN,
                        Message.Value.newBuilder()
                                .setIntValue(
                                        QueryFlowOuterClass.ColumnType.valueOf(
                                                        "COLUMN_"
                                                                + StringUtils.upperCase(
                                                                        column.name()))
                                                .getNumber())
                                .addAllIntValueList(labelIdList)
                                .addAllStrValueList(labelNameList));
        return parseSingleUnaryVertex(
                contextManager.getVertexIdManager(),
                contextManager.getTreeNodeLabelManager(),
                processorFunction,
                contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        ValueType inputValueType = getInputNode().getOutputValueType();
        if (inputValueType instanceof MapValueType) {
            if (column == Column.keys) {
                return new ListValueType(((MapValueType) inputValueType).getKey());
            } else {
                return new ListValueType(((MapValueType) inputValueType).getValue());
            }
        } else if (inputValueType instanceof MapEntryValueType) {
            if (column == Column.keys) {
                return ((MapEntryValueType) inputValueType).getKey();
            } else {
                return ((MapEntryValueType) inputValueType).getValue();
            }
        } else if (inputValueType instanceof PathValueType) {
            if (column == Column.keys) {
                return new ListValueType(
                        new ListValueType(new ValueValueType(Message.VariantType.VT_STRING)));
            } else {
                return new ListValueType(((PathValueType) inputValueType).getPathValue());
            }
        } else {
            throw new IllegalArgumentException(
                    "Not support column operator for value type=>" + inputValueType);
        }
    }

    public Column getColumn() {
        return column;
    }
}
