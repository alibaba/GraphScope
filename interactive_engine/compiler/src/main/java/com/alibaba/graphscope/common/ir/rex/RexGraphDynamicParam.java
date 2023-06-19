/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;

public class RexGraphDynamicParam extends RexDynamicParam {
    private final String paramName;

    public RexGraphDynamicParam(RelDataType type, String paramName, int index) {
        super(type, index);
        this.paramName = paramName;
    }

    @Override
    public String getName() {
        return paramName;
    }
}
