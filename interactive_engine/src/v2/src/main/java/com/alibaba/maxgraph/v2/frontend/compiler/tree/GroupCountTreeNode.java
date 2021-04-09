package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.proto.v2.VariantType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.JoinZeroNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.source.SourceDelegateNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.MapValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.CompilerUtils;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.TreeNodeUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;

public class GroupCountTreeNode extends UnaryTreeNode {
    private TreeNode keyTreeNode = null;

    public GroupCountTreeNode(TreeNode prev, GraphSchema schema) {
        super(prev, NodeType.AGGREGATE, schema);
    }

    public void setKeyTreeNode(TreeNode keyTreeNode) {
        this.keyTreeNode = keyTreeNode;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);
        LogicalVertex outputVertex;

        if (null == keyTreeNode || keyTreeNode instanceof SourceDelegateNode) {
            ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.GROUP_COUNT);
            outputVertex = new LogicalUnaryVertex(
                    contextManager.getVertexIdManager().getId(),
                    processorFunction,
                    false,
                    sourceVertex);
            logicalSubQueryPlan.addLogicalVertex(outputVertex);
            logicalSubQueryPlan.addLogicalEdge(sourceVertex, outputVertex, new LogicalEdge());
        } else {
            TreeNode currentKeyNode = TreeNodeUtils.buildSingleOutputNode(keyTreeNode, schema);
            if (currentKeyNode instanceof JoinZeroNode) {
                ((JoinZeroNode) currentKeyNode).disableJoinZero();
            }
            LogicalSubQueryPlan keyValuePlan = TreeNodeUtils.buildSubQueryPlan(currentKeyNode, sourceVertex, contextManager);
            LogicalVertex groupValueVertex = keyValuePlan.getOutputVertex();
            LogicalVertex enterKeyVertex = TreeNodeUtils.getSourceTreeNode(currentKeyNode).getOutputVertex();
            logicalSubQueryPlan.mergeLogicalQueryPlan(keyValuePlan);
            if (TreeNodeUtils.checkJoinSourceFlag(currentKeyNode)) {
                String valueLabel = contextManager.getTreeNodeLabelManager().createSysLabelStart("val");
                getUsedLabelList().add(valueLabel);
                int valueLabelId = contextManager.getTreeNodeLabelManager().getLabelIndex(valueLabel);
                LogicalBinaryVertex logicalBinaryVertex = new LogicalBinaryVertex(
                        contextManager.getVertexIdManager().getId(),
                        new ProcessorFunction(CompilerUtils.parseJoinOperatorType(currentKeyNode), Value.newBuilder().setIntValue(valueLabelId)),
                        false,
                        enterKeyVertex,
                        groupValueVertex);
                logicalSubQueryPlan.addLogicalVertex(logicalBinaryVertex);
                logicalSubQueryPlan.addLogicalEdge(enterKeyVertex, logicalBinaryVertex, new LogicalEdge());
                logicalSubQueryPlan.addLogicalEdge(groupValueVertex, logicalBinaryVertex, new LogicalEdge());

                ProcessorFunction selectValueFunction = TreeNodeUtils.createSelectOneFunction(valueLabel, Pop.first, contextManager.getTreeNodeLabelManager().getLabelIndexList());
                LogicalVertex selectLabelVertex = new LogicalUnaryVertex(
                        contextManager.getVertexIdManager().getId(),
                        selectValueFunction,
                        false,
                        logicalBinaryVertex);
                logicalSubQueryPlan.addLogicalVertex(selectLabelVertex);
                logicalSubQueryPlan.addLogicalEdge(logicalBinaryVertex, selectLabelVertex, LogicalEdge.forwardEdge());
            }

            groupValueVertex = logicalSubQueryPlan.getOutputVertex();
            ProcessorFunction processorFunction = new ProcessorFunction(
                    OperatorType.GROUP_COUNT,
                    Value.newBuilder());
            outputVertex = new LogicalUnaryVertex(contextManager.getVertexIdManager().getId(), processorFunction, false, groupValueVertex);
            logicalSubQueryPlan.addLogicalVertex(outputVertex);
            logicalSubQueryPlan.addLogicalEdge(groupValueVertex, outputVertex, new LogicalEdge());

        }

        ProcessorFunction foldMapFunction = new ProcessorFunction(OperatorType.FOLDMAP);
        LogicalVertex foldMapVertex = new LogicalUnaryVertex(contextManager.getVertexIdManager().getId(), foldMapFunction, false, outputVertex);
        logicalSubQueryPlan.addLogicalVertex(foldMapVertex);
        logicalSubQueryPlan.addLogicalEdge(outputVertex, foldMapVertex, new LogicalEdge());

        addUsedLabelAndRequirement(foldMapVertex, contextManager.getTreeNodeLabelManager());
        setFinishVertex(foldMapVertex, contextManager.getTreeNodeLabelManager());

        return logicalSubQueryPlan;
    }

    @Override
    public boolean isPropLocalFlag() {
        return false;
    }

    @Override
    public ValueType getOutputValueType() {
        return new MapValueType(null == keyTreeNode ? getInputNode().getOutputValueType() : keyTreeNode.getOutputValueType(), new ValueValueType(VariantType.VT_LONG));
    }
}
