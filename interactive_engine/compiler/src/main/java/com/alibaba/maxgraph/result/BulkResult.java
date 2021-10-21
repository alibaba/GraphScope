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

import java.util.List;

public class BulkResult implements QueryResult {
    private List<QueryResult> resultList;

    public BulkResult(List<QueryResult> resultList) {
        this.resultList = resultList;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("resultList", resultList)
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BulkResult that = (BulkResult) o;
        return Objects.equal(resultList, that.resultList);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(resultList);
    }

    public List<QueryResult> getResultList() {
        return resultList;
    }

}
