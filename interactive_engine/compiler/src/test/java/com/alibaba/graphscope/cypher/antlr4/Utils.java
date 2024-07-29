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

package com.alibaba.graphscope.cypher.antlr4;

import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;
import com.alibaba.graphscope.cypher.antlr4.visitor.GraphBuilderVisitor;
import com.alibaba.graphscope.cypher.antlr4.visitor.LogicalPlanVisitor;

public abstract class Utils {
    public static final GraphBuilder eval(String query) {
        GraphBuilder graphBuilder = com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder();
        return new GraphBuilderVisitor(graphBuilder).visit(new CypherAntlr4Parser().parse(query));
    }

    public static final GraphBuilder eval(String query, GraphBuilder builder) {
        return new GraphBuilderVisitor(builder).visit(new CypherAntlr4Parser().parse(query));
    }

    public static LogicalPlan evalLogicalPlan(String query) {
        GraphBuilder graphBuilder = com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder();
        LogicalPlanVisitor logicalPlanVisitor =
                new LogicalPlanVisitor(
                        graphBuilder, com.alibaba.graphscope.common.ir.Utils.schemaMeta);
        return logicalPlanVisitor.visit(new CypherAntlr4Parser().parse(query));
    }

    public static LogicalPlan evalLogicalPlan(String query, String schemaPath) {
        GraphBuilder graphBuilder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(schemaPath);
        IrMeta irMeta = com.alibaba.graphscope.common.ir.Utils.mockSchemaMeta(schemaPath);
        LogicalPlanVisitor logicalPlanVisitor = new LogicalPlanVisitor(graphBuilder, irMeta);
        return logicalPlanVisitor.visit(new CypherAntlr4Parser().parse(query));
    }
}
