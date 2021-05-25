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
package com.alibaba.maxgraph.v2.frontend.compiler.tree.addition;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.VariantType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueValueType;

public interface CountFlagNode {
    boolean checkCountOptimize();

    void enableCountFlag();

    boolean isCountFlag();

    default OperatorType getCountOperator(OperatorType operatorType) {
        if (isCountFlag()) {
            return OperatorType.valueOf(operatorType + "_COUNT");
        } else {
            return operatorType;
        }
    }

    default ValueType getCountOutputType(ValueType valueType) {
        if (isCountFlag()) {
            return new ValueValueType(VariantType.VT_LONG);
        } else {
            return valueType;
        }
    }
}
