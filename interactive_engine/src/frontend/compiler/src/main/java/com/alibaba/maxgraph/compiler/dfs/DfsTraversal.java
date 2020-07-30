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
package com.alibaba.maxgraph.compiler.dfs;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.tree.DfsFinishTreeNode;
import com.alibaba.maxgraph.compiler.tree.DfsRepeatGraphTreeNode;
import com.alibaba.maxgraph.compiler.tree.NodeType;
import com.alibaba.maxgraph.compiler.tree.OrderGlobalTreeNode;
import com.alibaba.maxgraph.compiler.tree.RangeGlobalTreeNode;
import com.alibaba.maxgraph.compiler.tree.RepeatTreeNode;
import com.alibaba.maxgraph.compiler.tree.TreeBuilder;
import com.alibaba.maxgraph.compiler.tree.TreeManager;
import com.alibaba.maxgraph.compiler.tree.TreeNode;
import com.alibaba.maxgraph.compiler.tree.UnaryTreeNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceDfsTreeNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceDelegateNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceTreeNode;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

public class DfsTraversal {
    private static final long MAX_BFS_ITERATION_TIMES = 1000000000;
    private GraphTraversal.Admin<?, ?> traversal;
    private long low;
    private long high;
    private long batchSize;
    private boolean order;

    public DfsTraversal(GraphTraversal.Admin<?, ?> traversal, long low, long high, long batchSize, boolean order) {
        this.traversal = traversal;
        this.low = low;
        this.high = high;
        this.order = order;
        this.batchSize = batchSize;
    }

    public DfsTraversal(GraphTraversal traversal, long low, long high, long batchSize, boolean order) {
        this(traversal.asAdmin(), low, high, batchSize, order);
    }

    public GraphTraversal.Admin<?, ?> getTraversal() {
        return traversal;
    }

    public long getLow() {
        return low;
    }

    public long getHigh() {
        return high;
    }

    public boolean isOrder() {
        return order;
    }

    public long getBatchSize() {
        return batchSize;
    }

    /**
     * Build bfs tree manager
     *
     * @param treeBuilder The tree builder
     * @param schema      The schema
     * @return The result tree managet
     */
    public TreeManager buildDfsTree(TreeBuilder treeBuilder, GraphSchema schema) {
        TreeManager treeManager = treeBuilder.build(traversal);
        treeManager.optimizeTree();

        TreeNode currentNode = treeManager.getTreeLeaf();
        while (!(currentNode instanceof SourceTreeNode)) {
            if (currentNode.getNodeType() == NodeType.AGGREGATE) {
                throw new IllegalArgumentException("There's aggregate in the query and can'e be executed in bfs mode");
            }
            currentNode = UnaryTreeNode.class.cast(currentNode).getInputNode();
        }
        SourceDfsTreeNode sourceBfsTreeNode = new SourceDfsTreeNode(schema, batchSize);
        RepeatTreeNode repeatTreeNode = new RepeatTreeNode(sourceBfsTreeNode, schema, Maps.newHashMap());

        TreeNode sourceOutputNode = currentNode.getOutputNode();
        SourceDelegateNode repeatSourceTreeNode = new SourceDelegateNode(sourceBfsTreeNode, schema);
        DfsRepeatGraphTreeNode dfsRepeatGraphTreeNode = new DfsRepeatGraphTreeNode(repeatSourceTreeNode, SourceTreeNode.class.cast(currentNode), schema);
        if (null != sourceOutputNode) {
            UnaryTreeNode.class.cast(sourceOutputNode).setInputNode(dfsRepeatGraphTreeNode);
        } else {
            treeManager.setLeafNode(dfsRepeatGraphTreeNode);
        }

        TreeNode bodyLeafTreeNode = treeManager.getTreeLeaf();
        repeatTreeNode.setRepeatBodyTreeNode(bodyLeafTreeNode);
        repeatTreeNode.setEmitTreeNode(new SourceDelegateNode(bodyLeafTreeNode, schema));
        if (order) {
            OrderGlobalTreeNode orderTreeNode = new OrderGlobalTreeNode(
                    new SourceDelegateNode(bodyLeafTreeNode, schema),
                    schema,
                    Lists.newArrayList(Pair.of(new SourceDelegateNode(bodyLeafTreeNode, schema), Order.incr)));
            repeatTreeNode.setDfsEmitTreeNode(orderTreeNode);
        }
        repeatTreeNode.setDfsFeedTreeNode(
                new DfsFinishTreeNode(
                        new SourceDelegateNode(bodyLeafTreeNode, schema),
                        schema,
                        high));
        repeatTreeNode.setMaxLoopTimes(MAX_BFS_ITERATION_TIMES);

        RangeGlobalTreeNode rangeGlobalTreeNode = new RangeGlobalTreeNode(repeatTreeNode, schema, low, high);
        return new TreeManager(rangeGlobalTreeNode, schema, treeManager.getLabelManager(), treeManager.getQueryConfig());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DfsTraversal that = (DfsTraversal) o;
        return low == that.low &&
                high == that.high &&
                batchSize == that.batchSize &&
                order == that.order &&
                Objects.equal(traversal, that.traversal);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(traversal, low, high, batchSize, order);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("traversal", traversal)
                .add("low", low)
                .add("high", high)
                .add("batchSize", batchSize)
                .add("order", order)
                .toString();
    }
}
