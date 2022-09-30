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

package com.alibaba.graphscope.gremlin.plugin.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;

import java.util.NoSuchElementException;

public class ExprStep<S, E> extends MapStep<S, E> {
    private String expr;
    private Type type;

    public ExprStep(Traversal.Admin traversal, String expr, Type type) {
        super(traversal);
        this.expr = expr;
        this.type = type;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        return null;
    }

    public String getExpr() {
        return expr;
    }

    public Type getType() {
        return type;
    }

    public enum Type {
        FILTER,
        PROJECTION
    }
}
