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
package com.alibaba.maxgraph.v2.common.frontend.result;

import com.google.common.base.MoreObjects;

import java.util.Set;

/**
 * Value in path result, contains value and label set
 */
public class PathValueResult implements QueryResult {
    private Object value;
    private Set<String> labelList;

    public PathValueResult() {

    }

    public PathValueResult(Object value) {
        this(value, null);
    }

    public PathValueResult(Object value, Set<String> labelList) {
        this.value = value;
        this.labelList = labelList;
    }

    public Object getValue() {
        return value;
    }

    public Set<String> getLabelList() {
        return labelList;
    }

    @Override
    public Object convertToGremlinStructure() {
        return new PathValueResult(
                this.value instanceof QueryResult ? ((QueryResult) this.value).convertToGremlinStructure() : this.value,
                this.labelList);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("value", value)
                .add("labelList", labelList)
                .toString();
    }
}
