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

package com.alibaba.graphscope.common.ir.tools;

import com.alibaba.graphscope.common.ir.meta.QueryMode;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class QueryModeVisitor extends RelVisitor {
    private QueryMode mode;
    private final List<Class<? extends RelNode>> writeOperations;

    public QueryModeVisitor() {
        this(ImmutableList.of());
    }

    public QueryModeVisitor(List<Class<? extends RelNode>> writeOperations) {
        this.writeOperations = writeOperations;
        this.mode = QueryMode.READ;
    }

    @Override
    public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
        if (writeOperations.stream().anyMatch(k -> k.isInstance(node))) {
            mode = QueryMode.WRITE;
            return;
        }
        node.childrenAccept(this);
    }

    public QueryMode getMode() {
        return mode;
    }
}
