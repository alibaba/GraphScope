/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.rel;

import com.alibaba.graphscope.common.ir.rel.graph.AbstractBindableTableScan;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.google.common.base.Preconditions;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;

public abstract class GraphRelShuttleWrapper extends RelShuttleImpl {
    protected final GraphRelShuttle relShuttle;

    protected GraphRelShuttleWrapper(GraphRelShuttle relShuttle) {
        this.relShuttle = relShuttle;
    }

    @Override
    public RelNode visit(TableScan tableScan) {
        Preconditions.checkArgument(
                tableScan instanceof AbstractBindableTableScan,
                "tableScan should be " + AbstractBindableTableScan.class);
        if (tableScan instanceof GraphLogicalSource) {
            return relShuttle.visit((GraphLogicalSource) tableScan);
        } else if (tableScan instanceof GraphLogicalExpand) {
            return relShuttle.visit((GraphLogicalExpand) tableScan);
        } else { // GraphLogicalGetV
            return relShuttle.visit((GraphLogicalGetV) tableScan);
        }
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        return relShuttle.visit(filter);
    }

    @Override
    public RelNode visit(RelNode relNode) {
        if (relNode instanceof GraphLogicalProject) {
            return relShuttle.visit((GraphLogicalProject) relNode);
        } else if (relNode instanceof GraphLogicalAggregate) {
            return relShuttle.visit((GraphLogicalAggregate) relNode);
        } else if (relNode instanceof GraphLogicalSort) {
            return relShuttle.visit((GraphLogicalSort) relNode);
        } else if (relNode instanceof GraphLogicalSingleMatch) {
            return relShuttle.visit((GraphLogicalSingleMatch) relNode);
        } else if (relNode instanceof GraphLogicalMultiMatch) {
            return relShuttle.visit((GraphLogicalMultiMatch) relNode);
        } else {
            throw new UnsupportedOperationException(
                    "relNode " + relNode.getClass() + " can not be visited in shuttle");
        }
    }
}
