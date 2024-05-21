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

package com.alibaba.graphscope.common.antlr4;

import com.alibaba.graphscope.common.ir.meta.QueryMode;

import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;

import java.util.List;

/**
 * parse DSL statement to antlr tree
 */
public abstract class Antlr4Parser {
    protected final List<Class<? extends ParseTree>> writeOperators;

    public Antlr4Parser(List<Class<? extends ParseTree>> writeOperators) {
        this.writeOperators = writeOperators;
    }

    public ParseResult parse(String statement) {
        ParseTree parseTree = parse0(statement);
        return new ParseResult(parseTree, (new QueryModeVisitor(writeOperators)).visit(parseTree));
    }

    protected abstract ParseTree parse0(String statement);

    // help to determine whether a query which denoted by AST tree contains any write operations
    static class QueryModeVisitor extends AbstractParseTreeVisitor<QueryMode> {
        public final List<Class<? extends ParseTree>> writeOperations;

        public QueryModeVisitor(List<Class<? extends ParseTree>> writeOperations) {
            this.writeOperations = writeOperations;
        }

        public QueryMode visitChildren(RuleNode node) {
            if (writeOperations.stream().anyMatch(k -> k.isInstance(node))) {
                return QueryMode.WRITE;
            }
            return super.visitChildren(node);
        }

        @Override
        protected QueryMode aggregateResult(QueryMode aggregate, QueryMode nextResult) {
            return (aggregate == QueryMode.WRITE || nextResult == QueryMode.WRITE)
                    ? QueryMode.WRITE
                    : QueryMode.READ;
        }

        @Override
        protected QueryMode defaultResult() {
            return QueryMode.READ;
        }
    }
}
