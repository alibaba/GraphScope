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
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.T;

public class PropertyValueResult implements QueryResult {
    private final Object value;

    public PropertyValueResult(Object value) {
        this.value = value;
    }

    public Object getValue() {
        if (value instanceof String) {
            String strValue = (String) value;
            if (StringUtils.equals(strValue, T.id.getAccessor())) {
                return T.id;
            } else if (StringUtils.equals(strValue, T.label.getAccessor())) {
                return T.label;
            }
        }
        return value;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .addValue(value).toString();
    }
}
