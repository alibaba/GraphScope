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
package com.alibaba.maxgraph.compiler.prepare;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.util.List;

public class PreparedExecuteParam {
    private String prepareId;
    private List<List<Object>> paramList;

    public PreparedExecuteParam(String preparedId, List<List<Object>> paramList) {
        this.prepareId = preparedId;
        this.paramList = paramList;
    }

    public String getPrepareId() {
        return prepareId;
    }

    public List<List<Object>> getParamList() {
        return paramList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PreparedExecuteParam that = (PreparedExecuteParam) o;
        return Objects.equal(prepareId, that.prepareId) &&
                Objects.equal(paramList, that.paramList);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(prepareId, paramList);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("prepareId", prepareId)
                .add("paramList", paramList)
                .toString();
    }
}
