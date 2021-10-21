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

import com.google.common.base.Objects;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.function.BiPredicate;

public class StringPredicate<S, E> extends CustomPredicate<S, E> implements NegatePredicate {
    private static final long serialVersionUID = 7057958862444567004L;
    private String content;
    private MatchType matchType;
    private boolean negate = false;

    public StringPredicate() {
        super(null, null);
        setPredicateType(PredicateType.STRING);
    }

    public StringPredicate(String content, MatchType matchType) {
        super(null, null);
        this.content = content;
        this.matchType = matchType;
        setPredicateType(PredicateType.STRING);
    }

    public String getContent() {
        return content;
    }

    public MatchType getMatchType() {
        return matchType;
    }

    @Override
    public BiPredicate<Traversal<S, E>, Traversal<S, E>> getBiPredicate() {
        throw new UnsupportedOperationException("Not this operation in gremlin=>getBiPredicate");
    }

    public Traversal<S, E> getOriginalValue() {
        throw new UnsupportedOperationException("Not this operation in gremlin=>getOriginalValue");
    }

    public Traversal<S, E> getValue() {
        throw new UnsupportedOperationException("Not this operation in gremlin=>getValue");
    }

    public void setValue(Traversal<S, E> value) {
        throw new UnsupportedOperationException("Not this operation in gremlin=>setValue");
    }

    @Override
    public boolean isNegate() {
        return negate;
    }

    @Override
    public boolean test(Traversal<S, E> traversal) {
        throw new UnsupportedOperationException("Not this operation in gremlin=>test");
    }

    @Override
    public StringPredicate<S, E> negate() {
        this.negate = true;
        return this;
    }

    @Override
    public String toString() {
        return matchType + "(" + content + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(content, matchType, negate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StringPredicate<?, ?> that = (StringPredicate<?, ?>) o;
        return com.google.common.base.Objects.equal(content, that.content) &&
                matchType == that.matchType &&
                negate == that.negate;
    }
}
