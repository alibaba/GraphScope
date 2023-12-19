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

import com.alibaba.graphscope.common.ir.rel.graph.*;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;

/**
 * the class is used to visit a graph relation tree
 */
public abstract class GraphRelVisitor extends RelShuttleImpl {
    public RelNode visit(GraphLogicalSource source) {
        return source;
    }

    public RelNode visit(GraphLogicalExpand expand) {
        return visitChildren(expand);
    }

    public RelNode visit(GraphLogicalExpandDegree degree) {
        return visitChildren(degree);
    }

    public RelNode visit(GraphLogicalGetV getV) {
        return visitChildren(getV);
    }

    public RelNode visit(GraphLogicalPathExpand expand) {
        return visitChildren(expand);
    }

    public RelNode visit(GraphLogicalSingleMatch match) {
        return match;
    }

    public RelNode visit(GraphLogicalMultiMatch match) {
        return match;
    }

    public RelNode visit(GraphLogicalAggregate aggregate) {
        return visitChildren(aggregate);
    }

    public RelNode visit(GraphLogicalProject project) {
        return visitChildren(project);
    }

    public RelNode visit(GraphLogicalSort sort) {
        return visitChildren(sort);
    }
}
