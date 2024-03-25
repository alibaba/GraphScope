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

package com.alibaba.graphscope.gremlin.antlr4x.visitor;

import static java.util.Objects.requireNonNull;

import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.rel.CommonTableScan;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.ObjectUtils;

/**
 * convert sub traversal nested in {@code NestedTraversalContext} to RelNode
 */
public class NestedTraversalRelVisitor extends GremlinGSBaseVisitor<RelNode> {
    private final GraphBuilder parentBuilder;
    private final GraphBuilder nestedBuilder;

    public NestedTraversalRelVisitor(GraphBuilder parentBuilder) {
        this.parentBuilder = parentBuilder;
        this.nestedBuilder =
                GraphBuilder.create(
                        this.parentBuilder.getContext(),
                        (GraphOptCluster) this.parentBuilder.getCluster(),
                        this.parentBuilder.getRelOptSchema());
    }

    @Override
    public RelNode visitNestedTraversal(GremlinGSParser.NestedTraversalContext ctx) {
        RelNode commonRel =
                requireNonNull(parentBuilder.peek(), "parent builder should not be empty");
        if (!isGlobalSource(commonRel)) {
            RelOptTable commonTable = new CommonOptTable(commonRel);
            commonRel =
                    new CommonTableScan(
                            commonRel.getCluster(), commonRel.getTraitSet(), commonTable);
        }
        nestedBuilder.push(commonRel);
        return new GraphBuilderVisitor(nestedBuilder).visitNestedTraversal(ctx).build();
    }

    private boolean isGlobalSource(RelNode rel) {
        if (rel instanceof GraphLogicalSource
                && ((GraphLogicalSource) rel).getTableConfig().isAll()) {
            GraphLogicalSource source = (GraphLogicalSource) rel;
            return source.getUniqueKeyFilters() == null && ObjectUtils.isEmpty(source.getFilters());
        }
        return false;
    }
}
