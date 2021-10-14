/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.sdkcommon.compiler.custom;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.function.BiPredicate;

public abstract class CustomPredicate<S, E> extends P<Traversal<S, E>> {
    private PredicateType predicateType;

    public CustomPredicate() {
        super(null, null);
    }

    public CustomPredicate(BiPredicate<Traversal<S, E>, Traversal<S, E>> biPredicate, Traversal<S, E> value) {
        super(null, null);
    }

    public PredicateType getPredicateType() {
        return predicateType;
    }

    public void setPredicateType(PredicateType predicateType) {
        this.predicateType = predicateType;
    }

    @Override
    public BiPredicate<Traversal<S, E>, Traversal<S, E>> getBiPredicate() {
        throw new IllegalArgumentException("no bi predicate in custom predicate");
    }

    public abstract int hashCode();

    public abstract boolean equals(final Object other);
}
