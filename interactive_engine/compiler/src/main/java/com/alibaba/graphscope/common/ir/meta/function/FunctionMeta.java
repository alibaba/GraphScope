/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.meta.function;

import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;

import org.apache.calcite.sql.type.*;

import java.util.stream.Collectors;

public class FunctionMeta {
    private final String signature;
    private final SqlReturnTypeInference returnTypeInference;
    private final SqlOperandTypeChecker operandTypeChecker;
    private final SqlOperandTypeInference operandTypeInference;

    public FunctionMeta(
            String signature,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeChecker operandTypeChecker,
            SqlOperandTypeInference operandTypeInference) {
        this.signature = signature;
        this.returnTypeInference = returnTypeInference;
        this.operandTypeChecker = operandTypeChecker;
        this.operandTypeInference = operandTypeInference;
    }

    public FunctionMeta(BuiltInFunction function) {
        this(
                function.getSignature(),
                function.getReturnTypeInference(),
                function.getOperandTypeChecker(),
                function.getOperandTypeInference());
    }

    public FunctionMeta(StoredProcedureMeta meta) {
        this(
                meta.getName(),
                ReturnTypes.explicit(meta.getReturnType().getFieldList().get(0).getType()),
                GraphOperandTypes.metaTypeChecker(meta),
                InferTypes.explicit(
                        meta.getParameters().stream()
                                .map(k -> k.getDataType())
                                .collect(Collectors.toList())));
    }

    public String getSignature() {
        return this.signature;
    }

    public SqlReturnTypeInference getReturnTypeInference() {
        return this.returnTypeInference;
    }

    public SqlOperandTypeChecker getOperandTypeChecker() {
        return this.operandTypeChecker;
    }

    public SqlOperandTypeInference getOperandTypeInference() {
        return this.operandTypeInference;
    }
}
