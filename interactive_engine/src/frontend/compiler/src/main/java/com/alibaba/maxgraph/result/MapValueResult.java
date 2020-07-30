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
package com.alibaba.maxgraph.result;

import com.alibaba.maxgraph.sdkcommon.graph.QueryResult;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import java.util.Map;

public class MapValueResult implements QueryResult {
    private Map<QueryResult, QueryResult> valueMap = Maps.newLinkedHashMap();

    public MapValueResult() {
    }

    public void addMapValue(QueryResult key, QueryResult value) {
        valueMap.put(key, value);
    }

    public Map<QueryResult, QueryResult> getValueMap() {
        return valueMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MapValueResult that = (MapValueResult) o;
        return Objects.equal(valueMap, that.valueMap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(valueMap);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("valueMap", valueMap)
                .toString();
    }
}
