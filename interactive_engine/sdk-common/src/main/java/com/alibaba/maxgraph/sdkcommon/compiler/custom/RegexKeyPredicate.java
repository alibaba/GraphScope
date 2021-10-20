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

import java.util.Objects;

public class RegexKeyPredicate<S, E> extends RegexPredicate<S, E> {
    private static final long serialVersionUID = 6074654239797217518L;
    private final String key;

    public RegexKeyPredicate(String key, String regex) {
        super(regex);
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, getRegex());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegexKeyPredicate<?, ?> that = (RegexKeyPredicate<?, ?>) o;
        return com.google.common.base.Objects.equal(key, that.key) && super.equals(o);
    }
}
