/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.cypher.antlr4.visitor;

import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.rel.CommonTableScan;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.grammar.CypherGSBaseVisitor;
import com.alibaba.graphscope.grammar.CypherGSParser;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

public class CallSubQueryVisitor extends CypherGSBaseVisitor<RelNode> {
    private final GraphBuilder parentBuilder;
    private final GraphBuilder nestedBuilder;

    public CallSubQueryVisitor(GraphBuilder parentBuilder) {
        this.parentBuilder = parentBuilder;
        this.nestedBuilder =
                GraphBuilder.create(
                        this.parentBuilder.getContext(),
                        (GraphOptCluster) this.parentBuilder.getCluster(),
                        this.parentBuilder.getRelOptSchema());
    }

    @Override
    public RelNode visitOC_CallSubQuery(CypherGSParser.OC_CallSubQueryContext ctx) {
        if (parentBuilder.size() > 0) {
            RelNode commonRel = parentBuilder.peek();
            RelOptTable commonTable = new CommonOptTable(commonRel);
            commonRel =
                    new CommonTableScan(
                            commonRel.getCluster(), commonRel.getTraitSet(), commonTable);
            nestedBuilder.push(commonRel);
        }
        return new GraphBuilderVisitor(nestedBuilder).visit(ctx.oC_SubQuery()).build();
    }
}
