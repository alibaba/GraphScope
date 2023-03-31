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

import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPathExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;

/**
 * interface to visit each {@code RelNode}
 */
public interface GraphRelShuttle {
    RelNode visit(GraphLogicalSource source);

    RelNode visit(GraphLogicalExpand expand);

    RelNode visit(GraphLogicalGetV getV);

    RelNode visit(GraphLogicalPathExpand expand);

    RelNode visit(LogicalFilter filter);

    RelNode visit(GraphLogicalProject project);

    RelNode visit(GraphLogicalAggregate aggregate);

    RelNode visit(GraphLogicalSort sort);

    RelNode visit(GraphLogicalSingleMatch match);

    RelNode visit(GraphLogicalMultiMatch match);
}
