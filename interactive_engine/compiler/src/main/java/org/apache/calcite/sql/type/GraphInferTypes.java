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

package org.apache.calcite.sql.type;

import com.alibaba.graphscope.common.ir.rex.RexCallBinding;
import com.google.common.base.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.Arrays;

public abstract class GraphInferTypes {
    private GraphInferTypes() {}

    /**
     * Operand type-inference strategy where an unknown operand type is derived
     * from the first operand with a known type.
     */
    public static final SqlOperandTypeInference FIRST_KNOWN =
            (callBinding, returnType, operandTypes) -> {
                Preconditions.checkArgument(callBinding instanceof RexCallBinding);
                RelDataType unknownType = callBinding.getTypeFactory().createUnknownType();
                RelDataType knownType = unknownType;
                for (RexNode rexNode : ((RexCallBinding) callBinding).getRexOperands()) {
                    knownType = rexNode.getType();
                    if (!knownType.equals(unknownType)) {
                        break;
                    }
                }
                Arrays.fill(operandTypes, knownType);
            };
}
