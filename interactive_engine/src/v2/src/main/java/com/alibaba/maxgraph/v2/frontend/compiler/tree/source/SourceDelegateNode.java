package com.alibaba.maxgraph.v2.frontend.compiler.tree.source;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.BaseTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;

public class SourceDelegateNode extends SourceTreeNode {
    private TreeNode delegate;
    private ValueType delegateOutputValueType = null;
    private boolean repeatFlag = false;

    public SourceDelegateNode(TreeNode delegate, GraphSchema schema) {
        super(null, schema);
        this.delegate = delegate;
    }

    public void enableRepeatFlag() {
        this.repeatFlag = true;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
       LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
       logicalSubQueryPlan.addLogicalVertex(getOutputVertex());
       return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        if (null == delegateOutputValueType) {
            return delegate.getOutputValueType();
        } else {
            return delegateOutputValueType;
        }
    }

    @Override
    public LogicalVertex getOutputVertex() {
        if (null == outputVertex) {
            return delegate.getOutputVertex();
        } else {
            return outputVertex;
        }
    }

    public void setDelegateOutputValueType(ValueType delegateOutputValueType) {
        this.delegateOutputValueType = delegateOutputValueType;
    }

    @Override
    public boolean isPropLocalFlag() {
        if (repeatFlag) {
            return false;
        }

        return delegate.isPropLocalFlag();
    }

    public void setPropLocalFlag(boolean propLocalFlag) {
        BaseTreeNode.class.cast(delegate).setPropLocalFlag(propLocalFlag);
    }

    public TreeNode getDelegate() {
        return delegate;
    }
}
