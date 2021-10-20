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
package com.alibaba.maxgraph.compiler.tree.source;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.BaseTreeNode;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.tree.TreeNode;

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
