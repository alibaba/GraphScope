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

import com.alibaba.graphscope.grammar.GremlinGSParser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.Objects;

public class TraversalMethodIterator implements Iterator<GremlinGSParser.TraversalMethodContext> {
    private GremlinGSParser.TraversalMethodContext next;
    private GremlinGSParser.TraversalSourceSpawnMethodContext sourceNext;

    public TraversalMethodIterator(GremlinGSParser.TraversalMethodContext next) {
        this.next = Objects.requireNonNull(next);
    }

    public TraversalMethodIterator(GremlinGSParser.TraversalSourceSpawnMethodContext sourceNext) {
        this.sourceNext = Objects.requireNonNull(sourceNext);
        this.next = null;
    }

    @Override
    public boolean hasNext() {
        if (this.next == null) {
            GremlinGSParser.RootTraversalContext rootCtx =
                    (GremlinGSParser.RootTraversalContext) sourceNext.getParent();
            return rootCtx != null && rootCtx.chainedTraversal() != null;
        }
        ParseTree parent = getParent(getParent(next));
        return parent != null && parent.getChildCount() >= 3;
    }

    @Override
    public GremlinGSParser.TraversalMethodContext next() {
        if (this.next == null) {
            GremlinGSParser.RootTraversalContext rootCtx =
                    (GremlinGSParser.RootTraversalContext) sourceNext.getParent();
            if (rootCtx == null || rootCtx.chainedTraversal() == null) return null;
            next = leftDfs(rootCtx.chainedTraversal());
            return next;
        }
        ParseTree parent = getParent(getParent(next));
        if (parent == null || parent.getChildCount() < 3) return null;
        next = (GremlinGSParser.TraversalMethodContext) parent.getChild(2);
        return next;
    }

    private @Nullable ParseTree getParent(ParseTree child) {
        Class<? extends ParseTree> parentClass = GremlinGSParser.ChainedTraversalContext.class;
        while (child != null
                && child.getParent() != null
                && !child.getParent().getClass().equals(parentClass)) {
            child = child.getParent();
        }
        return (child != null
                        && child.getParent() != null
                        && child.getParent().getClass().equals(parentClass))
                ? child.getParent()
                : null;
    }

    private GremlinGSParser.TraversalMethodContext leftDfs(
            GremlinGSParser.ChainedTraversalContext root) {
        if (root.chainedTraversal() != null) {
            return leftDfs(root.chainedTraversal());
        }
        return root.traversalMethod();
    }
}
