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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.Objects;

public class RegexPredicate<S, E> extends CustomPredicate<S, E> {
    private static final long serialVersionUID = 7057958862444567004L;
    private String regex;

    public RegexPredicate() {
        super();
        setPredicateType(PredicateType.REGEX);
    }

    public RegexPredicate(String regex) {
        super(null, null);
        this.regex = regex;
        setPredicateType(PredicateType.REGEX);
    }

    public String getRegex() {
        return regex;
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
    public boolean test(Traversal<S, E> traversal) {
        throw new UnsupportedOperationException("Not this operation in gremlin=>test");
    }

    @Override
    public int hashCode() {
        return Objects.hash(regex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegexPredicate<?, ?> that = (RegexPredicate<?, ?>) o;
        return com.google.common.base.Objects.equal(regex, that.regex);
    }
}
