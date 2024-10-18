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

import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphPathType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.common.ir.type.GraphTypeFamily;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.type.*;

import java.util.List;
import java.util.stream.Collectors;

public enum BuiltInFunction {
    GS_FUNCTION_RELATIONSHIPS(
            "gs.function.relationships",
            // set return type inference of function 'gs.function.relationships':
            // 1. get first parameter type which should be `GraphPathType`
            // 2. get the inner edge type of `GraphPathType`
            // 3. convert to array type which has the edge type as the component
            ReturnTypes.ARG0
                    .andThen(
                            (op, type) -> {
                                GraphPathType.ElementType elementType =
                                        (GraphPathType.ElementType) type.getComponentType();
                                return elementType.getExpandType();
                            })
                    .andThen(SqlTypeTransforms.TO_ARRAY),
            // set operand type checker of function 'gs.function.relationships'
            GraphOperandTypes.family(GraphTypeFamily.PATH),
            GraphInferTypes.FIRST_KNOWN),

    GS_FUNCTION_NODES(
            "gs.function.nodes",
            ReturnTypes.ARG0
                    .andThen(
                            (op, type) -> {
                                GraphPathType.ElementType elementType =
                                        (GraphPathType.ElementType) type.getComponentType();
                                return elementType.getGetVType();
                            })
                    .andThen(SqlTypeTransforms.TO_ARRAY),
            GraphOperandTypes.family(GraphTypeFamily.PATH),
            GraphInferTypes.FIRST_KNOWN),

    GS_FUNCTION_START_NODE(
            "gs.function.startNode",
            ReturnTypes.ARG0.andThen(
                    (op, type) -> {
                        GraphSchemaType edgeType = (GraphSchemaType) type;
                        List<GraphLabelType.Entry> srcLabels =
                                edgeType.getLabelType().getLabelsEntry().stream()
                                        .map(
                                                e ->
                                                        new GraphLabelType.Entry()
                                                                .label(e.getSrcLabel())
                                                                .labelId(e.getSrcLabelId()))
                                        .collect(Collectors.toList());
                        return new GraphSchemaType(
                                GraphOpt.Source.VERTEX,
                                new GraphLabelType(srcLabels),
                                ImmutableList.of());
                    }),
            GraphOperandTypes.family(GraphTypeFamily.EDGE),
            GraphInferTypes.FIRST_KNOWN),

    GS_FUNCTION_END_NODE(
            "gs.function.endNode",
            ReturnTypes.ARG0.andThen(
                    (op, type) -> {
                        GraphSchemaType edgeType = (GraphSchemaType) type;
                        List<GraphLabelType.Entry> endLabels =
                                edgeType.getLabelType().getLabelsEntry().stream()
                                        .map(
                                                e ->
                                                        new GraphLabelType.Entry()
                                                                .label(e.getDstLabel())
                                                                .labelId(e.getDstLabelId()))
                                        .collect(Collectors.toList());
                        return new GraphSchemaType(
                                GraphOpt.Source.VERTEX,
                                new GraphLabelType(endLabels),
                                ImmutableList.of());
                    }),
            GraphOperandTypes.family(GraphTypeFamily.EDGE),
            GraphInferTypes.FIRST_KNOWN);

    BuiltInFunction(
            String signature,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeChecker operandTypeChecker,
            SqlOperandTypeInference operandTypeInference) {
        this.signature = signature;
        this.returnTypeInference = returnTypeInference;
        this.operandTypeChecker = operandTypeChecker;
        this.operandTypeInference = operandTypeInference;
    }

    private final String signature;
    private final SqlReturnTypeInference returnTypeInference;
    private final SqlOperandTypeChecker operandTypeChecker;
    private final SqlOperandTypeInference operandTypeInference;

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
