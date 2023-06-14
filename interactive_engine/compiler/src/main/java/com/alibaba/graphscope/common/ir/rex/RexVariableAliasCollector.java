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

import com.google.common.collect.ImmutableList;

import org.apache.calcite.rex.*;

import java.util.List;
import java.util.function.Function;

/**
 * collect all {@code aliasIds} of each {@code RexVariable} in an expression
 */
public class RexVariableAliasCollector<R> extends RexVisitorImpl<List<R>> {
    private final Function<RexGraphVariable, R> collectFunc;

    public RexVariableAliasCollector(boolean deep, Function<RexGraphVariable, R> collectFunc) {
        super(deep);
        this.collectFunc = collectFunc;
    }

    @Override
    public List<R> visitCall(RexCall call) {
        ImmutableList.Builder builder = ImmutableList.builder();
        if (this.deep) {
            call.getOperands()
                    .forEach(
                            k -> {
                                builder.addAll(k.accept(this));
                            });
        }
        return builder.build();
    }

    @Override
    public List<R> visitLiteral(RexLiteral literal) {
        return ImmutableList.of();
    }

    @Override
    public List<R> visitInputRef(RexInputRef inputRef) {
        return (inputRef instanceof RexGraphVariable)
                ? visitGraphVariable((RexGraphVariable) inputRef)
                : ImmutableList.of();
    }

    public List<R> visitGraphVariable(RexGraphVariable variable) {
        return ImmutableList.of(collectFunc.apply(variable));
    }

    @Override
    public List<R> visitDynamicParam(RexDynamicParam dynamicParam) {
        return ImmutableList.of();
    }
}
