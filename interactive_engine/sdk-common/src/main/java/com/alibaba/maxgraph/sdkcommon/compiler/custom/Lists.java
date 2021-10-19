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

public final class Lists {
    public static <V> ListPredicate contains(V value) {
        return new ListPredicate<>(value, ListMatchType.LIST_CONTAINS);
    }

    public static <V> ListKeyPredicate contains(String key, V value) {
        return new ListKeyPredicate<>(key, value, ListMatchType.LIST_CONTAINS);
    }

    public static <V> ListPredicate containsAny(java.util.List<V> valueList) {
        return new ListPredicate<>(valueList, ListMatchType.LIST_CONTAINS_ANY);
    }

    public static <V> ListKeyPredicate containsAny(String key, java.util.List<V> valueList) {
        return new ListKeyPredicate<>(key, valueList, ListMatchType.LIST_CONTAINS_ANY);
    }

    public static <V> ListPredicate containsAll(java.util.List<V> valueList) {
        return new ListPredicate<>(valueList, ListMatchType.LIST_CONTAINS_ALL);
    }

    public static <V> ListKeyPredicate containsAll(String key, java.util.List<V> valueList) {
        return new ListKeyPredicate<>(key, valueList, ListMatchType.LIST_CONTAINS_ALL);
    }

    public static <V> java.util.List<V> of(V... val) {
        return com.google.common.collect.Lists.newArrayList(val);
    }
}
