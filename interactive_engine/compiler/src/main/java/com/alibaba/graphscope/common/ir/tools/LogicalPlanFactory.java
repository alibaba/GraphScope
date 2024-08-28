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

package com.alibaba.graphscope.common.ir.tools;

import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.utils.ClassUtils;
import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;
import com.alibaba.graphscope.cypher.antlr4.visitor.LogicalPlanVisitor;
import com.alibaba.graphscope.gremlin.antlr4x.parser.GremlinAntlr4Parser;
import com.alibaba.graphscope.gremlin.antlr4x.visitor.GraphBuilderVisitor;
import com.alibaba.graphscope.proto.frontend.Code;

import org.antlr.v4.runtime.tree.ParseTree;

public interface LogicalPlanFactory {
    LogicalPlan create(GraphBuilder builder, IrMeta irMeta, String query);

    class Cypher implements LogicalPlanFactory {
        @Override
        public LogicalPlan create(GraphBuilder builder, IrMeta irMeta, String query) {
            ParseTree cypherAST =
                    ClassUtils.callException(
                            () -> new CypherAntlr4Parser().parse(query),
                            Code.CYPHER_INVALID_SYNTAX);
            return ClassUtils.callException(
                    () -> new LogicalPlanVisitor(builder, irMeta).visit(cypherAST),
                    Code.LOGICAL_PLAN_BUILD_FAILED);
        }
    }

    class Gremlin implements LogicalPlanFactory {
        @Override
        public LogicalPlan create(GraphBuilder builder, IrMeta irMeta, String query) {
            ParseTree gremlinAST =
                    ClassUtils.callException(
                            () -> new GremlinAntlr4Parser().parse(query),
                            Code.GREMLIN_INVALID_SYNTAX);
            return ClassUtils.callException(
                    () ->
                            new LogicalPlan(
                                    new GraphBuilderVisitor(builder).visit(gremlinAST).build()),
                    Code.LOGICAL_PLAN_BUILD_FAILED);
        }
    }
}
